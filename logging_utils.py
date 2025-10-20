from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


LOG_DIR = Path("LOGs")
LOG_FORMAT_FILE = "%(asctime)s | %(levelname)-8s | %(message)s"
LOG_FORMAT_CONSOLE = "%(asctime)s | %(levelname)-8s | %(message)s"
FILE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
CONSOLE_TIME_FORMAT = "%H:%M:%S"


def setup_logging(
    name: str,
    *,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """Configure and return a dedicated logger with console and file handlers."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)

    # Remove existing handlers to avoid duplicate logs when reconfiguring.
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(LOG_DIR / f"{name}.log", encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(
        logging.Formatter(LOG_FORMAT_FILE, FILE_TIME_FORMAT)
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(
        logging.Formatter(LOG_FORMAT_CONSOLE, CONSOLE_TIME_FORMAT)
    )

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    logger.debug(
        "Logger '%s' configured (console_level=%s, file_level=%s)",
        name,
        logging.getLevelName(console_level),
        logging.getLevelName(file_level),
    )
    return logger

