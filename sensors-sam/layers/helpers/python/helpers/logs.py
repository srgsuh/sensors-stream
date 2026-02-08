# logs.py
import logging
import os
import sys

DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", default="INFO").upper()

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"

def _configure_logging() -> None:
    root_logger = logging.getLogger()

    if root_logger.handlers:
        return

    level = getattr(logging, DEBUG_LEVEL, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))

    root_logger.setLevel(level)
    root_logger.addHandler(handler)


_configure_logging()

logger = logging.getLogger("app")

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
