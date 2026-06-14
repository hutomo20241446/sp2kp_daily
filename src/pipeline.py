# src/pipeline.py
"""
Orchestrator ETL pipeline SP2KP (daily, GitHub Actions).

Alur:
1. Cek apakah tanggal target sudah ada di DB → skip jika sudah lengkap.
2. Ambil daftar kabupaten dari situs (fallback ke hardcoded).
3. Jalankan N worker paralel; tiap worker scrape satu (kab, date) per iterasi.
4. Transform in-memory (clean + dedup).
5. Upsert ke Supabase, urut tanggal ASC.
"""

import asyncio
import time
from asyncio import Queue
from datetime import date

from src.config.settings import Settings
from src.config.logger import logger
from src.scraper.browser_factory import BrowserFactory
from src.scraper.page_session import PageSession
from src.scraper.sp2kp_scraper import SP2KPScraper, EXPECTED_KOMODITAS
from src.scraper.entities import HargaHarian
from src.transform.transformer import transform
from src.load.supabase_loader import upsert_to_supabase


# ── Guard: cek apakah tanggal sudah lengkap di DB ──────────────────────────────

def _is_date_complete_in_db(
    dsn: str,
    target: date,
    kabupaten_count: int
) -> bool:

    expected = kabupaten_count * EXPECTED_KOMODITAS

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:

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

        complete = filled >= expected

        logger.info(
            f"DB check {target}: "
            f"{filled}/{expected} harga terisi "
            f"→ {'LENGKAP' if complete else 'BELUM LENGKAP'}"
        )

        return complete

    except Exception as e:
        logger.warning(
            f"DB check gagal ({e}), lanjut scraping."
        )
        return False


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

        # 1. Ambil daftar kabupaten
        kabupaten_list = await scraper.fetch_kabupaten(browser)
        total = len(kabupaten_list)

        # 2. Guard: skip jika tanggal sudah lengkap di DB
        if _is_date_complete_in_db(settings.dsn, settings.target_date, total):
            logger.info("Data sudah lengkap di DB, pipeline selesai tanpa scraping.")
            return

        logger.info(f"  Kabupaten : {total}")
        logger.info(f"  Workers   : {settings.workers} tab paralel")
        logger.info("=" * 60)

        # 3. Isi queue
        task_queue: Queue = Queue()
        for kab in kabupaten_list:
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
    logger.info(f"  Berhasil : {counter['done']}")
    logger.info(f"  Gagal    : {counter['failed']}")
    logger.info(f"  Raw rows : {len(raw_results)}")
    logger.info("=" * 60)

    if not raw_results:
        logger.warning(
            "Tidak ada data berhasil di-scrape. "
            "Kemungkinan SP2KP tidak upload data hari ini — ini wajar."
        )
        return

    # 5. Transform
    clean_records = transform(raw_results)

    # 6. Upsert ke Supabase
    result = upsert_to_supabase(settings.dsn, clean_records)

    elapsed_total = time.time() - t_start
    logger.info("=" * 60)
    logger.info("✅ Pipeline selesai!")
    logger.info(f"   Upserted : {result['upserted']} rows")
    logger.info(f"   Skipped  : {result['skipped']} rows (key tidak ditemukan)")
    logger.info(f"   Tanggal  : {result['dates']}")
    logger.info(f"   Total    : {elapsed_total / 60:.1f} menit")
    logger.info("=" * 60)
