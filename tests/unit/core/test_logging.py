import io
import json
from typing import Any

import structlog

from autotrader.core.logging import bind_cycle, configure_logging


def _lines(buf: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in buf.getvalue().splitlines()]


def test_one_json_line_per_event_with_required_keys() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)

    structlog.get_logger().info("feed.connected", symbols=3)

    (line,) = _lines(buf)
    assert line["event"] == "feed.connected"
    assert line["level"] == "info"
    assert line["symbols"] == 3
    assert line["timestamp"].endswith("Z")  # UTC ISO-8601
    assert line["cycle_id"] is None  # present on every line, even outside a cycle


def test_bind_cycle_adds_cycle_id_and_restores() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    log = structlog.get_logger()

    with bind_cycle(142):
        log.info("inside")
        with bind_cycle(143):
            log.info("nested")
        log.info("inside.again")
    log.info("outside")

    assert [line["cycle_id"] for line in _lines(buf)] == [142, 143, 142, None]


def test_level_filters_below_threshold() -> None:
    buf = io.StringIO()
    configure_logging(level="warning", stream=buf)
    log = structlog.get_logger()

    log.info("dropped")
    log.warning("kept")

    assert [line["event"] for line in _lines(buf)] == ["kept"]


def test_exceptions_are_rendered_into_the_line() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)

    try:
        raise RuntimeError("boom")
    except RuntimeError:
        structlog.get_logger().exception("handler.failed")

    (line,) = _lines(buf)
    assert "RuntimeError: boom" in line["exception"]
