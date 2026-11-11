"""Suite-wide fixtures."""

from collections.abc import Iterator

import pytest
import structlog


@pytest.fixture(autouse=True)
def _restore_structlog_config() -> Iterator[None]:
    """Give every test back the structlog configuration it started with.

    structlog's configuration is process-global. A test that reaches `configure_logging` (a
    CliRunner invocation of a command, or a direct call) binds the logger factory to a stream
    that is closed once the test ends, so every later test that logs would fail with
    `I/O operation on closed file` - the outcome would depend on test order.
    """
    saved = structlog.get_config()
    try:
        yield
    finally:
        structlog.configure(**saved)
