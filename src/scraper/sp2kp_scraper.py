# src/scraper/sp2kp_scraper.py
from datetime import date
from playwright.async_api import Browser

from src.scraper.entities import HargaHarian
from src.scraper.page_session import PageSession
from src.config.logger import logger


# Jumlah komoditas parent-row yang diharapkan per kabupaten per tanggal
# (update Mei 2026: +1 Beras SPHP Bulog → total 17)
EXPECTED_KOMODITAS = 17

# Fallback hardcoded jika fetch dari dropdown situs gagal
KABUPATEN_JATENG_FALLBACK = [
    "Kab. Banjarnegara", "Kab. Banyumas",   "Kab. Batang",
    "Kab. Blora",        "Kab. Boyolali",    "Kab. Brebes",
    "Kab. Cilacap",      "Kab. Demak",       "Kab. Grobogan",
    "Kab. Jepara",       "Kab. Karanganyar", "Kab. Kebumen",
    "Kab. Kendal",       "Kab. Klaten",      "Kab. Kudus",
    "Kab. Magelang",     "Kab. Pati",        "Kab. Pekalongan",
    "Kab. Pemalang",     "Kab. Purbalingga", "Kab. Purworejo",
    "Kab. Rembang",      "Kab. Semarang",    "Kab. Sragen",
    "Kab. Sukoharjo",    "Kab. Tegal",       "Kab. Temanggung",
    "Kab. Wonogiri",     "Kab. Wonosobo",
    "Kota Magelang",     "Kota Pekalongan",  "Kota Salatiga",
    "Kota Semarang",     "Kota Surakarta",   "Kota Tegal",
]


def _clean_price(raw: str) -> float | None:
    """'Rp 12.500' → 12500.0  |  '-' / '' → None"""
    cleaned = (
        raw.replace("Rp", "")
           .replace(".", "")
           .replace(",", ".")
           .strip()
    )
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


class SP2KPScraper:
    def __init__(self, base_url: str, provinsi: str):
        self.base_url = base_url
        self.provinsi = provinsi

    # ── 1. Ambil daftar kabupaten ──────────────────────────────────

    async def fetch_kabupaten(self, browser: Browser) -> list[str]:
        logger.info("Mengambil daftar kabupaten dari situs …")
        session = PageSession(browser, self.base_url, self.provinsi, worker_id=0)
        await session.open()
        page = session.page

        try:
            kab_input = page.locator("#input-23")
            await kab_input.click()
            await page.wait_for_timeout(1_000)

            # Scroll dropdown agar semua item termuat
            for _ in range(8):
                try:
                    await page.locator(".v-overlay__content .v-list").evaluate(
                        "el => el.scrollTop += 500"
                    )
                except Exception:
                    pass
                await page.wait_for_timeout(200)

            items = await page.locator(".v-overlay__content .v-list-item").all()
            result = [
                t for t in [(await i.inner_text()).strip() for i in items] if t
            ]
            await page.keyboard.press("Escape")

        finally:
            await session.close()

        if result:
            logger.info(f"✓ {len(result)} kabupaten/kota ditemukan dari situs")
            return result

        logger.warning(
            f"⚠ Gagal ambil dari situs, pakai fallback "
            f"({len(KABUPATEN_JATENG_FALLBACK)} kab/kota)"
        )
        return KABUPATEN_JATENG_FALLBACK

    # ── 2. Scrape satu tugas (kab, date) ──────────────────────────

    async def scrape_task(
        self,
        session: PageSession,
        kab: str,
        target: date,
        page_delay: float = 1.0,
    ) -> list[HargaHarian]:
        page   = session.page
        d_iso  = target.strftime("%Y-%m-%d")
        d_lbl  = target.strftime("%d/%m/%Y")

        logger.debug(f"Scraping {kab} | {d_lbl}")

        # -- Pilih kabupaten --
        kab_input = page.locator("#input-23")
        await kab_input.click(click_count=3)
        await page.wait_for_timeout(150)
        await kab_input.fill("")
        await page.wait_for_timeout(150)
        await kab_input.fill(kab)
        await page.wait_for_timeout(700)

        kab_opt = (
            page.locator(".v-overlay__content .v-list-item")
            .filter(has_text=kab)
            .first
        )
        await kab_opt.wait_for(state="visible", timeout=5_000)
        await kab_opt.click()
        await page.wait_for_timeout(300)

        # -- Isi tanggal (awal = akhir = tanggal target) --
        date_inputs = page.locator("input[type='date']")
        await date_inputs.nth(0).fill(d_iso)
        await date_inputs.nth(1).fill(d_iso)
        await date_inputs.nth(1).press("Tab")
        await page.wait_for_timeout(300)

        # Tunggu loading indicator
        try:
            await page.wait_for_selector(
                ".v-progress-linear", state="visible", timeout=2_000
            )
            await page.wait_for_selector(
                ".v-progress-linear", state="hidden", timeout=15_000
            )
        except Exception:
            pass

        await page.wait_for_timeout(page_delay * 1_000)
        await page.wait_for_selector("table tbody tr", timeout=10_000)

        # Double-check stabilitas tabel
        count_before = await page.locator("table tbody tr").count()
        await page.wait_for_timeout(300)
        count_after  = await page.locator("table tbody tr").count()
        if count_before != count_after:
            await page.wait_for_timeout(1_500)

        rows_el = await page.locator("table tbody tr").all()

        results: list[HargaHarian] = []
        for row in rows_el:
            # Skip child-rows breakdown region A/B/C (Beras Medium/Premium/SPHP)
            row_class = await row.get_attribute("class") or ""
            if "child-row" in row_class:
                continue

            cols = await row.locator("td").all()
            if len(cols) < 3:
                continue

            komoditas  = (await cols[0].inner_text()).strip()
            unit       = (await cols[1].inner_text()).strip()
            harga_raw  = (await cols[2].inner_text()).strip()

            if not komoditas:
                continue

            results.append(
                HargaHarian(
                    provinsi       = self.provinsi,
                    kabupaten_kota = kab,
                    komoditas      = komoditas,
                    unit           = unit,
                    tanggal        = target,
                    harga          = _clean_price(harga_raw),
                )
            )

        logger.debug(f"  {kab} | {d_lbl} → {len(results)} komoditas")
        return results
