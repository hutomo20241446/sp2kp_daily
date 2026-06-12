# src/config/logger.py
import logging
import sys
from datetime import datetime


def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logger = logging.getLogger("sp2kp")
    logger.info(f"Logger dimulai — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return logger


logger = setup_logger()
