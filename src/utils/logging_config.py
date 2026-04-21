"""Structured JSON logging configuration.

Why structured logging?
- Databricks job logs stream to Log Analytics / CloudWatch; JSON parses cleanly there
- Every event carries context (tenant, layer, batch_id) for slicing without regex
- One log() call instead of manual f-string formatting, less drift risk
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    """Configure structlog + stdlib logging to emit JSON or console output.

    Args:
        level: Log level (DEBUG / INFO / WARNING / ERROR).
        fmt: 'json' for production, 'console' for local readability.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if fmt == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str, **initial_context: Any) -> structlog.stdlib.BoundLogger:
    """Get a bound logger with baseline context.

    Example:
        log = get_logger(__name__, layer="bronze", tenant_id="tenant_a")
        log.info("ingestion_started", file_count=42)
    """
    logger = structlog.get_logger(name)
    if initial_context:
        logger = logger.bind(**initial_context)
    return logger
