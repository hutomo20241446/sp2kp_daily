# src/load/supabase_loader.py
"""
Loader: upsert HargaHarian ke Supabase (PostgreSQL via psycopg3).

Alur upsert:
1. Query tanggal terbaru di fact_harga_harian (untuk logging / validasi).
2. Kelompokkan records per tanggal, urutkan tanggal ASC
   → tanggal terbaru selalu di-upsert terakhir (sesuai permintaan).
3. Upsert dim_wilayah dan dim_komoditas (ON CONFLICT DO NOTHING).
4. Fetch surrogate keys.
5. Upsert fact_harga_harian per tanggal, urut ASC.
"""

from collections import defaultdict
from datetime import date

import psycopg

from src.scraper.entities import HargaHarian
from src.config.logger import logger


# ── helpers ────────────────────────────────────────────────────────────────────


def _get_latest_date_in_db(cur) -> date | None:
    """Kembalikan tanggal terbaru di fact_harga_harian, atau None kalau kosong."""
    cur.execute("SELECT MAX(tanggal) FROM fact_harga_harian")
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _upsert_dim_wilayah(cur, records: list[HargaHarian]):
    pairs = list({(r.provinsi, r.kabupaten_kota) for r in records})
    cur.executemany(
        """
        INSERT INTO dim_wilayah (provinsi, kabupaten_kota)
        VALUES (%s, %s)
        ON CONFLICT (provinsi, kabupaten_kota) DO NOTHING
        """,
        pairs,
    )


def _upsert_dim_komoditas(cur, records: list[HargaHarian]):
    pairs = list({(r.komoditas, r.unit) for r in records})
    cur.executemany(
        """
        INSERT INTO dim_komoditas (komoditas, unit)
        VALUES (%s, %s)
        ON CONFLICT (komoditas, unit) DO NOTHING
        """,
        pairs,
    )


def _fetch_wilayah_lookup(cur) -> dict:
    cur.execute("SELECT wilayah_key, provinsi, kabupaten_kota FROM dim_wilayah")
    return {(r[1], r[2]): r[0] for r in cur.fetchall()}


def _fetch_komoditas_lookup(cur) -> dict:
    cur.execute("SELECT komoditas_key, komoditas, unit FROM dim_komoditas")
    return {(r[1], r[2]): r[0] for r in cur.fetchall()}


# ── main upsert ────────────────────────────────────────────────────────────────


def upsert_to_supabase(dsn: str, records: list[HargaHarian]) -> dict:
    """
    Upsert records ke Supabase.

    Urutan upsert:
    - records dikelompokkan per tanggal
    - tanggal di-sort ASC → tanggal terbaru masuk terakhir

    Mengembalikan dict ringkasan { upserted, skipped, dates }.
    """
    if not records:
        logger.info("Tidak ada record untuk di-upsert.")
        return {"upserted": 0, "skipped": 0, "dates": []}

    # Kelompokkan per tanggal, sort ASC
    by_date: dict[date, list[HargaHarian]] = defaultdict(list)
    for r in records:
        by_date[r.tanggal].append(r)
    sorted_dates = sorted(by_date.keys())  # ASC → terbaru terakhir

    logger.info(
        f"Akan upsert {len(records)} records "
        f"untuk {len(sorted_dates)} tanggal: "
        f"{[str(d) for d in sorted_dates]}"
    )

    total_upserted = 0
    total_skipped  = 0

    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:

            # -- Info DB sebelum upsert --
            latest_db = _get_latest_date_in_db(cur)
            logger.info(
                f"Tanggal terbaru di DB sebelum upsert: "
                f"{latest_db if latest_db else '(kosong)'}"
            )

            # -- Upsert dimensi (sekali untuk semua records) --
            _upsert_dim_wilayah(cur, records)
            _upsert_dim_komoditas(cur, records)
            conn.commit()

            # -- Fetch surrogate keys --
            wilayah_lookup   = _fetch_wilayah_lookup(cur)
            komoditas_lookup = _fetch_komoditas_lookup(cur)

        # -- Upsert fact per tanggal (ASC) --
        for tgl in sorted_dates:
            day_records = by_date[tgl]
            fact_rows   = []
            warn_count  = 0

            for r in day_records:
                w_key = wilayah_lookup.get((r.provinsi, r.kabupaten_kota))
                k_key = komoditas_lookup.get((r.komoditas, r.unit))

                if w_key is None or k_key is None:
                    warn_count += 1
                    logger.warning(
                        f"Surrogate key tidak ditemukan: "
                        f"{r.provinsi} / {r.kabupaten_kota} / "
                        f"{r.komoditas} / {r.unit}"
                    )
                    continue

                fact_rows.append((tgl, w_key, k_key, r.harga))

            if warn_count:
                total_skipped += warn_count

            if not fact_rows:
                logger.warning(f"  {tgl}: tidak ada baris valid, skip.")
                continue

            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO fact_harga_harian (
                        tanggal, wilayah_key, komoditas_key, harga
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (tanggal, wilayah_key, komoditas_key)
                    DO UPDATE SET harga = EXCLUDED.harga
                    """,
                    fact_rows,
                )
            conn.commit()

            logger.info(
                f"  ✓ {tgl}: {len(fact_rows)} baris di-upsert ke fact_harga_harian"
            )
            total_upserted += len(fact_rows)

        # -- Info DB setelah upsert --
        with conn.cursor() as cur:
            latest_after = _get_latest_date_in_db(cur)
        logger.info(
            f"Tanggal terbaru di DB setelah upsert: "
            f"{latest_after if latest_after else '(kosong)'}"
        )

    return {
        "upserted": total_upserted,
        "skipped":  total_skipped,
        "dates":    [str(d) for d in sorted_dates],
    }
