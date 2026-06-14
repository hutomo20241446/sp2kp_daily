# src/load/supabase_loader.py
"""
Loader: upsert HargaHarian ke Supabase (PostgreSQL via psycopg3).

Alur upsert:
1. Query tanggal terbaru di fact_harga_harian (untuk logging / validasi).
2. Kelompokkan records per tanggal, urutkan tanggal ASC
   → tanggal terbaru selalu di-upsert terakhir.
3. Upsert dim_wilayah dan dim_komoditas.
4. Fetch surrogate keys.
5. Upsert fact_harga_harian per tanggal.
"""

from collections import defaultdict
from datetime import date

import psycopg

from src.scraper.entities import HargaHarian
from src.config.logger import logger


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _get_latest_date_in_db(cur) -> date | None:
    """Kembalikan tanggal terbaru di fact_harga_harian."""
    cur.execute("SELECT MAX(tanggal) FROM fact_harga_harian")
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _get_null_count(cur, target_date: date) -> int:
    """Hitung jumlah harga NULL pada tanggal tertentu."""
    cur.execute(
        """
        SELECT COUNT(*)
        FROM fact_harga_harian
        WHERE tanggal = %s
          AND harga IS NULL
        """,
        (target_date,),
    )
    return cur.fetchone()[0]


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
    cur.execute(
        """
        SELECT wilayah_key, provinsi, kabupaten_kota
        FROM dim_wilayah
        """
    )

    return {
        (provinsi, kabupaten_kota): wilayah_key
        for wilayah_key, provinsi, kabupaten_kota in cur.fetchall()
    }


def _fetch_komoditas_lookup(cur) -> dict:
    cur.execute(
        """
        SELECT komoditas_key, komoditas, unit
        FROM dim_komoditas
        """
    )

    return {
        (komoditas, unit): komoditas_key
        for komoditas_key, komoditas, unit in cur.fetchall()
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def upsert_to_supabase(dsn: str, records: list[HargaHarian]) -> dict:
    """
    Upsert records ke Supabase.

    Strategy:
    - Data dikelompokkan per tanggal.
    - Tanggal diurutkan ASC.
    - Tanggal terbaru selalu diproses terakhir.

    Return:
    {
        "upserted": int,
        "skipped": int,
        "dates": list[str]
    }
    """

    if not records:
        logger.info("Tidak ada record untuk di-upsert.")
        return {
            "upserted": 0,
            "skipped": 0,
            "dates": [],
        }

    # Kelompokkan per tanggal
    by_date: dict[date, list[HargaHarian]] = defaultdict(list)

    for record in records:
        by_date[record.tanggal].append(record)

    sorted_dates = sorted(by_date.keys())

    logger.info(
        f"Akan upsert {len(records)} records "
        f"untuk {len(sorted_dates)} tanggal: "
        f"{[str(d) for d in sorted_dates]}"
    )

    total_upserted = 0
    total_skipped = 0

    with psycopg.connect(dsn, autocommit=False) as conn:

        # ---------------------------------------------------------------------
        # Persiapan dimensi
        # ---------------------------------------------------------------------
        with conn.cursor() as cur:

            latest_before = _get_latest_date_in_db(cur)

            logger.info(
                "Tanggal terbaru di DB sebelum upsert: "
                f"{latest_before if latest_before else '(kosong)'}"
            )

            _upsert_dim_wilayah(cur, records)
            _upsert_dim_komoditas(cur, records)

            conn.commit()

            wilayah_lookup = _fetch_wilayah_lookup(cur)
            komoditas_lookup = _fetch_komoditas_lookup(cur)

        # ---------------------------------------------------------------------
        # Upsert fact per tanggal
        # ---------------------------------------------------------------------
        for tgl in sorted_dates:

            day_records = by_date[tgl]
            fact_rows = []
            warn_count = 0

            for r in day_records:

                wilayah_key = wilayah_lookup.get(
                    (r.provinsi, r.kabupaten_kota)
                )

                komoditas_key = komoditas_lookup.get(
                    (r.komoditas, r.unit)
                )

                if wilayah_key is None or komoditas_key is None:
                    warn_count += 1

                    logger.warning(
                        "Surrogate key tidak ditemukan: "
                        f"{r.provinsi} / "
                        f"{r.kabupaten_kota} / "
                        f"{r.komoditas} / "
                        f"{r.unit}"
                    )
                    continue

                fact_rows.append(
                    (
                        tgl,
                        wilayah_key,
                        komoditas_key,
                        r.harga,
                    )
                )

            total_skipped += warn_count

            if not fact_rows:
                logger.warning(
                    f"{tgl}: tidak ada baris valid, skip."
                )
                continue

            with conn.cursor() as cur:

                cur.executemany(
                    """
                    INSERT INTO fact_harga_harian (
                        tanggal,
                        wilayah_key,
                        komoditas_key,
                        harga
                    )
                    VALUES (%s, %s, %s, %s)

                    ON CONFLICT (
                        tanggal,
                        wilayah_key,
                        komoditas_key
                    )
                    DO UPDATE SET
                        harga = COALESCE(
                            EXCLUDED.harga,
                            fact_harga_harian.harga
                        )
                    """,
                    fact_rows,
                )

            conn.commit()

            total_upserted += len(fact_rows)

            logger.info(
                f"✓ {tgl}: "
                f"{len(fact_rows)} baris di-upsert"
            )

        # ---------------------------------------------------------------------
        # Validasi akhir
        # ---------------------------------------------------------------------
        with conn.cursor() as cur:

            latest_after = _get_latest_date_in_db(cur)

            logger.info(
                "Tanggal terbaru di DB setelah upsert: "
                f"{latest_after if latest_after else '(kosong)'}"
            )

            if latest_after:
                null_count = _get_null_count(
                    cur,
                    latest_after,
                )

                logger.info(
                    f"{latest_after}: "
                    f"sisa {null_count} harga NULL"
                )

    return {
        "upserted": total_upserted,
        "skipped": total_skipped,
        "dates": [str(d) for d in sorted_dates],
    }
