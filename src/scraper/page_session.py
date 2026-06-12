# src/scraper/page_session.py
import asyncio
from playwright.async_api import Browser, Page

from src.config.logger import logger

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
)


class PageSession:
    """Satu tab browser untuk satu worker — reusable lintas tugas."""

    def __init__(self, browser: Browser, base_url: str, provinsi: str, worker_id: int):
        self.browser   = browser
        self.base_url  = base_url
        self.provinsi  = provinsi
        self.worker_id = worker_id
        self.page: Page = None

    async def open(self):
        self.page = await self.browser.new_page(user_agent=USER_AGENT)
        # Stagger startup: worker 1=0s, worker 2=3s, dst.
        await asyncio.sleep(self.worker_id * 3)

        logger.debug(f"[W{self.worker_id}] Navigasi ke {self.base_url}")
        await self.page.goto(
            self.base_url,
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        await self.page.wait_for_timeout(2_000)
        await self._select_provinsi()
        logger.info(f"[W{self.worker_id}] ✓ Page siap ({self.provinsi})")

    async def reload_and_reset(self):
        """Reload dan pilih ulang provinsi — dipakai saat retry."""
        logger.debug(f"[W{self.worker_id}] Reload dan reset provinsi")
        await self.page.reload(wait_until="domcontentloaded", timeout=120_000)
        await self.page.wait_for_timeout(2_000)
        await self._select_provinsi()

    async def close(self):
        if self.page:
            await self.page.close()

    async def _select_provinsi(self):
        prov_input = self.page.locator("#input-20")
        await prov_input.click()
        await self.page.wait_for_timeout(800)
        await prov_input.fill(self.provinsi)
        await self.page.wait_for_timeout(1500)
        opt = (
            self.page.locator(".v-list-item")
            .filter(has_text=self.provinsi)
            .first
        )
        await opt.wait_for(state="visible", timeout=15_000)
        await opt.click()
        await self.page.wait_for_timeout(1_500)
