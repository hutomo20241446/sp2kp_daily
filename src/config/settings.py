# src/config/settings.py
import os
from dataclasses import dataclass
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
    workers:    int   = 3
    page_delay: float = 1.0
    retry_max:  int   = 5
    headless:   bool  = True

    # DB
    db_host:     str = ""
    db_port:     str = "5432"
    db_name:     str = ""
    db_user:     str = ""
    db_password: str = ""

    @property
    def dsn(self) -> str:
        """
        Gunakan format key-value (libpq keyword) bukan URI.

        Alasan: DB_USER dari Supabase Pooler mengandung titik
        (contoh: postgres.jgzkecfbrsfpfqzdtrky). Format URI
        (postgresql://user:pass@host/db) dapat salah mem-parse
        username yang mengandung karakter non-alfanumerik,
        menyebabkan koneksi gagal.

        Format key-value aman untuk semua karakter di username/password.
        """
        return (
            f"host={self.db_host} "
            f"port={self.db_port} "
            f"dbname={self.db_name} "
            f"user={self.db_user} "
            f"password={self.db_password} "
            f"sslmode=require"
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
        workers     = int(os.getenv("WORKERS", "3")),
        page_delay  = float(os.getenv("PAGE_DELAY", "1.0")),
        retry_max   = int(os.getenv("RETRY_MAX", "5")),
        headless    = os.getenv("HEADLESS", "true").lower() == "true",
        db_host     = "aws-1-ap-southeast-2.pooler.supabase.com",
        db_port     = "5432",
        db_name     = "postgres",
        db_user     = "postgres.jgzkecfbrsfpfqzdtrky", 
        db_password = os.getenv("DB_PASSWORD", ""),
    )
    
