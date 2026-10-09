"""Structured JSON logging (SPEC §4.11): one line per event, `cycle_id` on every line."""

import logging
import sys
from collections.abc import Iterator, MutableMapping
from contextlib import contextmanager
from typing import Any, TextIO

import structlog


def _default_cycle_id(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    event_dict.setdefault("cycle_id", None)
    return event_dict


def configure_logging(level: str = "info", stream: TextIO | None = None) -> None:
    """Configure structlog for JSON lines on `stream` (default stdout)."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _default_cycle_id,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[level.upper()]
        ),
        logger_factory=structlog.PrintLoggerFactory(file=stream or sys.stdout),
        cache_logger_on_first_use=False,
    )


@contextmanager
def bind_cycle(cycle_id: int | None) -> Iterator[None]:
    """Attach `cycle_id` to every log line in this context; restores the outer value."""
    tokens = structlog.contextvars.bind_contextvars(cycle_id=cycle_id)
    try:
        yield
    finally:
        structlog.contextvars.reset_contextvars(**tokens)
