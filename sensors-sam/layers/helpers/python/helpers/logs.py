import logging
import os
import sys

DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", default="INFO").upper()
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
APP_PREFIX = "app"

def _configure_logging() -> None:
    app_logger = logging.getLogger(APP_PREFIX)

    if app_logger.handlers:
        return

    level = getattr(logging, DEBUG_LEVEL, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))

    app_logger.setLevel(level)
    app_logger.addHandler(handler)
    app_logger.propagate = False

_configure_logging()

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"{APP_PREFIX}.{name}")
