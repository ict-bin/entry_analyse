"""Logging utilities for secflow-app-entry-analyse."""

from __future__ import annotations

import logging
import sys
from typing import Any


def configure_container_logging(service_name: str = "entry_analyse") -> logging.Logger:
    """Configure structured logging for the container."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{service_name}] %(name)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    return logging.getLogger(service_name)


def log_event(
    logger: logging.Logger,
    level: int,
    message: str,
    **kwargs: Any,
) -> None:
    """Log a structured event with optional key-value context."""
    if kwargs:
        ctx = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
        logger.log(level, "%s | %s", message, ctx)
    else:
        logger.log(level, message)
