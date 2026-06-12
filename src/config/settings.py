# src/config/settings.py
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta


WIB = timezone(timedelta(hours=7))


def _today_wib() -> date:
    """Tanggal hari ini dalam zona waktu WIB (UTC+7)."""
    return datetime.now(tz=WIB).date()


@dataclass
class Settings:
    # Tanggal target scraping (satu hari)
    target_date: date

    # Scraper
    base_url:   str   = "https://sp2kp.kemendag.go.id"
    provinsi:   str   = "Jawa Tengah"
    workers:    int   = 5
    page_delay: float = 1.0
    retry_max:  int   = 3
    headless:   bool  = True

    # DB
    db_host:     str = ""
    db_port:     str = "5432"
    db_name:     str = ""
    db_user:     str = ""
    db_password: str = ""

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
            f"?sslmode=require"
        )


def get_settings() -> Settings:
    raw_date = os.getenv("TARGET_DATE", "").strip()
    if raw_date:
        target = date.fromisoformat(raw_date)
    else:
        target = _today_wib()

    return Settings(
        target_date = target,
        base_url    = os.getenv("BASE_URL",    "https://sp2kp.kemendag.go.id"),
        provinsi    = os.getenv("PROVINSI",    "Jawa Tengah"),
        workers     = int(os.getenv("WORKERS",    "5")),
        page_delay  = float(os.getenv("PAGE_DELAY", "1.0")),
        retry_max   = int(os.getenv("RETRY_MAX",  "3")),
        headless    = os.getenv("HEADLESS", "true").lower() == "true",
        db_host     = os.getenv("DB_HOST",     ""),
        db_port     = os.getenv("DB_PORT",     "5432"),
        db_name     = os.getenv("DB_NAME",     ""),
        db_user     = os.getenv("DB_USER",     ""),
        db_password = os.getenv("DB_PASSWORD", ""),
    )
