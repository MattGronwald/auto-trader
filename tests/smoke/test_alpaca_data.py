"""Smoke tests for Alpaca crypto market data. Opt-in: `uv run pytest -m smoke`. Read-only."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from autotrader.broker.alpaca_data import AlpacaBarSource
from autotrader.core.config import Settings
from autotrader.core.types import Bar, Quote

pytestmark = pytest.mark.smoke


@pytest.fixture
def source() -> AlpacaBarSource:
    settings = Settings()
    if settings.alpaca_api_key_id is None or settings.alpaca_api_secret_key is None:
        pytest.skip("Alpaca keys not configured")
    return AlpacaBarSource.from_settings(settings, ["BTC/USD"])


async def test_history_returns_recent_minute_bars(source: AlpacaBarSource) -> None:
    end = datetime.now(UTC).replace(second=0, microsecond=0)

    bars = await source.history(["BTC/USD"], end - timedelta(hours=2), end)

    assert bars, "BTC/USD should trade at least once in two hours"
    assert all(b.timeframe == "1m" and b.ts.second == 0 for b in bars)
    assert [b.ts for b in bars] == sorted(b.ts for b in bars)


async def test_latest_bar_and_quote(source: AlpacaBarSource) -> None:
    latest = await source.latest("BTC/USD")

    assert latest.bar_end is not None
    assert latest.quote_ts is not None


async def test_stream_connects(source: AlpacaBarSource) -> None:
    # Connection only: data on this venue is too sparse to wait for deterministically.
    async def ignore_bar(_: Bar) -> None: ...
    async def ignore_quote(_: Quote) -> None: ...

    task = asyncio.create_task(source.stream(ignore_bar, ignore_quote))
    try:
        async with asyncio.timeout(20):
            while not source.connected():
                await asyncio.sleep(0.2)
    finally:
        await source.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
