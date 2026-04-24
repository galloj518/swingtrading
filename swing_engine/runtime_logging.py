"""
Small structured logging helpers for deterministic scan runs.
"""
from __future__ import annotations
from typing import Optional

import json
import logging

from . import config as cfg


LOGGER_NAME = "swing_engine"


def configure_logging(level:Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, (level or cfg.LOG_LEVEL or "INFO").upper(), logging.INFO))
    logger.propagate = False
    return logger


def get_logger() -> logging.Logger:
    return configure_logging()


def log_event(logger: logging.Logger, level: int, event: str, **fields) -> None:
    payload = {"event": event}
    payload.update({key: value for key, value in fields.items() if value is not None})
    logger.log(level, json.dumps(payload, sort_keys=True, default=str))
