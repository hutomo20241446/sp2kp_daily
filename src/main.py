# src/main.py
import asyncio
from dotenv import load_dotenv

from src.config.settings import get_settings
from src.config.logger import logger
from src.pipeline import run_pipeline


def main():
    # .env hanya dipakai saat development lokal; di GitHub Actions env sudah tersedia
    load_dotenv()

    settings = get_settings()

    logger.info("=" * 60)
    logger.info("SP2KP Daily ETL — dimulai")
    logger.info(f"Target date : {settings.target_date}")
    logger.info(f"Provinsi    : {settings.provinsi}")
    logger.info(f"Workers     : {settings.workers}")
    logger.info("=" * 60)

    asyncio.run(run_pipeline(settings))


if __name__ == "__main__":
    main()
