# src/pipeline.py
"""
Orchestrator ETL pipeline SP2KP (daily, GitHub Actions).

Alur:
1. Cek apakah tanggal target sudah lengkap di DB (>= 595 harga non-null).
   Jika belum, identifikasi kabupaten yang BELUM memiliki data → hanya scrape itu.
2. Ambil daftar kabupaten dari situs (fallback ke hardcoded).
3. Jalankan N worker paralel; tiap worker scrape satu (kab, date) per iterasi.
4. Transform in-memory (clean + dedup).
5. Upsert ke Supabase (hanya non-null), urut tanggal ASC.
6. Log ringkasan: berapa diupsert, apakah sudah >= 595.
"""

import asyncio
import time
from asyncio import Queue
from datetime import date
import psycopg

from src.config.settings import Settings
from src.config.logger import logger
from src.scraper.browser_factory import BrowserFactory
from src.scraper.page_session import PageSession
from src.scraper.sp2kp_scraper import SP2KPScraper, EXPECTED_KOMODITAS
from src.scraper.entities import HargaHarian
from src.transform.transformer import transform
from src.load.supabase_loader import upsert_to_supabase, COMPLETE_THRESHOLD


# ── Guard: cek kelengkapan dan kabupaten yang belum masuk ─────────────────────

def _check_completion_status(
    dsn: str,
    target: date,
    kabupaten_list: list[str],
    provinsi: str,
) -> tuple[bool, list[str]]:
    """
    Cek apakah data tanggal target sudah lengkap di DB.

    Return:
        (is_complete, missing_kabupaten)

        - is_complete        : True jika harga non-null >= COMPLETE_THRESHOLD
        - missing_kabupaten  : daftar kabupaten yang belum punya data non-null
                               untuk tanggal target. Kosong jika is_complete=True.
    """
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:

                # Total harga non-null hari ini
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_harga_harian
                    WHERE tanggal = %s
                      AND harga IS NOT NULL
                    """,
                    (target,),
                )
                filled = cur.fetchone()[0]

                is_complete = filled >= COMPLETE_THRESHOLD

                logger.info(
                    f"DB check {target}: "
                    f"{filled}/{COMPLETE_THRESHOLD} harga terisi "
                    f"→ {'✅ LENGKAP' if is_complete else '⏳ BELUM LENGKAP'}"
                )

                if is_complete:
                    return True, []

                # Cari kabupaten yang SUDAH punya minimal 1 harga non-null
                cur.execute(
                    """
                    SELECT DISTINCT dw.kabupaten_kota
                    FROM fact_harga_harian fhh
                    JOIN dim_wilayah dw USING (wilayah_key)
                    WHERE fhh.tanggal = %s
                      AND fhh.harga IS NOT NULL
                      AND dw.provinsi = %s
                    """,
                    (target, provinsi),
                )
                done_set = {row[0] for row in cur.fetchall()}

        missing = [kab for kab in kabupaten_list if kab not in done_set]

        logger.info(
            f"Kabupaten sudah ada data : {len(done_set)}, "
            f"belum ada data : {len(missing)}"
        )
        if missing:
            logger.info(f"Kabupaten yang akan di-scrape ulang: {missing}")

        return False, missing

    except Exception as e:
        logger.warning(
            f"DB check gagal ({e}), lanjut scraping semua kabupaten."
        )
        return False, kabupaten_list


# ── Worker ──────────────────────────────────────────────────────────────────────

async def _worker(
    worker_id: int,
    settings: Settings,
    scraper: SP2KPScraper,
    browser,
    task_queue: Queue,
    results: list,
    results_lock: asyncio.Lock,
    counter: dict,
    total: int,
):
    session = PageSession(
        browser, settings.base_url, settings.provinsi, worker_id
    )
    await session.open()

    while True:
        try:
            kab = task_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        success = False
        for attempt in range(1, settings.retry_max + 1):
            try:
                rows = await scraper.scrape_task(
                    session, kab, settings.target_date,
                    page_delay=settings.page_delay,
                )

                if len(rows) != EXPECTED_KOMODITAS:
                    raise ValueError(
                        f"Komoditas tidak valid: dapat {len(rows)}, "
                        f"ekspektasi {EXPECTED_KOMODITAS}"
                    )

                async with results_lock:
                    results.extend(rows)
                    counter["done"] += 1
                    pct = counter["done"] / total * 100
                    logger.info(
                        f"[W{worker_id}] [{counter['done']:3d}/{total}] "
                        f"{pct:5.1f}% | {kab} | {settings.target_date} "
                        f"✓ {len(rows)} komoditas"
                    )

                success = True
                break

            except ValueError as e:
                logger.warning(
                    f"[W{worker_id}] ✗ attempt {attempt}/{settings.retry_max} "
                    f"{kab}: {e}"
                )

            except Exception as e:
                logger.warning(
                    f"[W{worker_id}] ✗ attempt {attempt}/{settings.retry_max} "
                    f"{kab}: {e}"
                )
                if attempt < settings.retry_max:
                    try:
                        await session.reload_and_reset()
                    except Exception as reload_err:
                        logger.warning(f"[W{worker_id}] ⚠ Reload gagal: {reload_err}")

        if not success:
            async with results_lock:
                counter["failed"] += 1
            logger.error(
                f"[W{worker_id}] ✗ SKIP {kab} setelah {settings.retry_max} retry"
            )

        task_queue.task_done()
        await asyncio.sleep(0.1)

    await session.close()
    logger.info(f"[Worker {worker_id}] selesai.")


# ── Orchestrator ────────────────────────────────────────────────────────────────

async def run_pipeline(settings: Settings):
    t_start = time.time()
    logger.info("=" * 60)
    logger.info(f"  SP2KP Daily ETL — target: {settings.target_date}")
    logger.info("=" * 60)

    scraper = SP2KPScraper(base_url=settings.base_url, provinsi=settings.provinsi)

    async with BrowserFactory(headless=settings.headless) as factory:
        browser = factory.browser

        # 1. Ambil daftar kabupaten lengkap dari situs
        kabupaten_list = await scraper.fetch_kabupaten(browser)
        total_kab = len(kabupaten_list)

        # 2. Guard: cek kelengkapan + tentukan mana yang belum masuk DB
        is_complete, to_scrape = _check_completion_status(
            dsn            = settings.dsn,
            target         = settings.target_date,
            kabupaten_list = kabupaten_list,
            provinsi       = settings.provinsi,
        )

        if is_complete:
            logger.info(
                f"✅ Data {settings.target_date} sudah lengkap "
                f"(>= {COMPLETE_THRESHOLD} harga non-null). "
                "Pipeline selesai tanpa scraping."
            )
            return

        if not to_scrape:
            # Harusnya tidak terjadi jika is_complete=False, tapi jaga-jaga
            logger.warning(
                "Tidak ada kabupaten yang perlu di-scrape. "
                "Sebagian kabupaten memang tidak meng-upload komoditas tertentu."
            )
            return

        total = len(to_scrape)
        logger.info(f"  Kabupaten yang akan di-scrape : {total}/{total_kab}")
        logger.info(f"  Workers                       : {settings.workers} tab paralel")
        logger.info("=" * 60)

        # 3. Isi queue hanya dengan kabupaten yang belum ada data
        task_queue: Queue = Queue()
        for kab in to_scrape:
            await task_queue.put(kab)

        raw_results: list[HargaHarian] = []
        results_lock = asyncio.Lock()
        counter = {"done": 0, "failed": 0}

        # 4. Jalankan workers paralel
        worker_tasks = [
            asyncio.create_task(
                _worker(
                    worker_id    = i + 1,
                    settings     = settings,
                    scraper      = scraper,
                    browser      = browser,
                    task_queue   = task_queue,
                    results      = raw_results,
                    results_lock = results_lock,
                    counter      = counter,
                    total        = total,
                )
            )
            for i in range(settings.workers)
        ]
        await asyncio.gather(*worker_tasks)

    elapsed_scrape = time.time() - t_start
    logger.info("=" * 60)
    logger.info(f"  Scraping selesai dalam {elapsed_scrape / 60:.1f} menit")
    logger.info(f"  Berhasil : {counter['done']} kabupaten")
    logger.info(f"  Gagal    : {counter['failed']} kabupaten")
    logger.info(f"  Raw rows : {len(raw_results)}")
    logger.info("=" * 60)

    if not raw_results:
        logger.warning(
            "Tidak ada data berhasil di-scrape. "
            "SP2KP tidak upload data hari ini."
        )
        return

    # 5. Filter null sebelum transform — hemat komputasi
    non_null_raw = [r for r in raw_results if r.harga is not None]
    null_count   = len(raw_results) - len(non_null_raw)

    logger.info(
        f"Filter harga: {len(non_null_raw)} non-null dilanjutkan, "
        f"{null_count} dibuang (harga=None) sebelum transform"
    )

    if not non_null_raw:
        logger.warning(
            "Semua records memiliki harga NULL — tidak ada yang di-upsert. "
            "SP2KP belum update data hari ini."
        )
        return

    # 6. Transform (hanya data non-null)
    clean_records = transform(non_null_raw)

    # 7. Upsert ke Supabase
    result = upsert_to_supabase(settings.dsn, clean_records)

    elapsed_total = time.time() - t_start

    # ── Ringkasan akhir ────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("✅ Pipeline selesai!")
    logger.info(f"   Upserted      : {result['upserted']} rows (non-null)")
    logger.info(f"   Skipped null  : {null_count} rows (harga=None, difilter sebelum transform)")
    logger.info(f"   Skipped key   : {result['skipped_key']} rows (surrogate key tidak ditemukan)")
    logger.info(f"   Tanggal       : {result['dates']}")
    logger.info(f"   Total waktu   : {elapsed_total / 60:.1f} menit")
    logger.info("─" * 60)

    # Cek apakah sudah mencapai threshold — jika belum, beri sinyal jadwal berikutnya
    tgl_str = str(settings.target_date)
    filled  = result["filled_after"].get(tgl_str, 0)
    complete = result["is_complete"].get(tgl_str, False)

    if complete:
        logger.info(
            f"✅ {tgl_str}: {filled}/{COMPLETE_THRESHOLD} harga terisi — DATA LENGKAP."
        )
    else:
        sisa = COMPLETE_THRESHOLD - filled
        logger.warning(
            f"⏳ {tgl_str}: {filled}/{COMPLETE_THRESHOLD} harga terisi "
            f"— MASIH KURANG {sisa}. "
            "Jadwal berikutnya akan scrape kabupaten yang belum masuk."
        )

    logger.info("=" * 60)
