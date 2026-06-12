# src/scraper/browser_factory.py
from playwright.async_api import async_playwright, Browser, Playwright


class BrowserFactory:
    """Async context manager untuk lifecycle browser Playwright."""

    def __init__(self, headless: bool = True):
        self.headless         = headless
        self._playwright: Playwright = None
        self.browser:    Browser    = None

    async def __aenter__(self) -> "BrowserFactory":
        self._playwright = await async_playwright().start()
        self.browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )
        return self

    async def __aexit__(self, *_):
        if self.browser:
            await self.browser.close()
        if self._playwright:
            await self._playwright.stop()
