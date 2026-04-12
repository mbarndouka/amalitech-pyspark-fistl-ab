"""
Structured logging setup using structlog.

Why structlog?
  - Outputs JSON in production (machine-parseable by Datadog, CloudWatch, etc.)
  - Outputs coloured console output in development (human-readable)
  - Supports context binding (request_id, pipeline_run_id) across the call stack
  - Zero-cost when log level is filtered out

Usage:
    from src.utils.logger import get_logger
    log = get_logger(__name__)
    log.info("ingestion_started", page=1, endpoint="/movie/popular")
"""

from __future__ import annotations

import logging
import sys
from structlog import configure
from structlog.types import EventDict, WrappedLogger

from config.settings import PipelineSettings, get_settings

def _add_severity(
        logger: WrappedLogger,
        method_name: str,
        event_dict: EventDict,
)-> EventDict:
    """Add severity to the log event."""
    event_dict["severity"] = method_name.upper()
    return event_dict

def configure_logger(settings: PipelineSettings) -> None:
    """Configure the logger."""
    settings = get_settings().pipeline
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    shared_processors: List[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        _add_severity,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.ExceptionRenderer(),
    ]

    if settings.logging_format == "json":
        shared_processors.append(structlog.processors.JSONRenderer())
    else:
        shared_processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=[*shared_processors, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)
    root_logger.addHandler(handler)


    for noisy in ("py4j", "pyspark","urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARN)

def get_logger(name: str) -> WrappedLogger:
    """Get a logger instance."""
    return structlog.get_logger(name)