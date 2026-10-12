"""AlpacaBarSource against fakes shaped like the raw payloads seen on 2026-10-11."""

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from msgpack import Timestamp

from autotrader.broker.alpaca_data import AlpacaBarSource
from autotrader.broker.base import BrokerError
from autotrader.core.types import Bar, Quote

T0 = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)


def raw_bar(minute: int, c: float = 100.0) -> dict[str, Any]:
    return {
        "t": (T0 + timedelta(minutes=minute)).isoformat().replace("+00:00", "Z"),
        "o": c,
        "h": c + 1,
        "l": c - 1,
        "c": c,
        "v": 0.5,
        "n": 2,
        "vw": c,
    }


class FakeHistorical:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.requests: list[Any] = []

    def get_crypto_bars(self, request: Any) -> dict[str, Any]:
        self.requests.append(request)
        if self.fail:
            raise ConnectionError("down")
        return {"BTC/USD": [raw_bar(1, 101), raw_bar(0, 100), raw_bar(5, 105)]}

    def get_crypto_latest_bar(self, request: Any) -> dict[str, Any]:
        return {"BTC/USD": raw_bar(3)}

    def get_crypto_latest_quote(self, request: Any) -> dict[str, Any]:
        return {"BTC/USD": {"t": "2026-10-11T10:04:30.5Z", "bp": 99.9, "ap": 100.1}}


class FakeStream:
    def __init__(self) -> None:
        self.bar_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None
        self.quote_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None
        self.subscribed: list[str] = []
        self._running = False
        self.stopped = False

    def subscribe_bars(
        self, handler: Callable[[dict[str, Any]], Awaitable[None]], *symbols: str
    ) -> None:
        self.bar_handler = handler
        self.subscribed += [f"bars:{s}" for s in symbols]

    def subscribe_quotes(
        self, handler: Callable[[dict[str, Any]], Awaitable[None]], *symbols: str
    ) -> None:
        self.quote_handler = handler
        self.subscribed += [f"quotes:{s}" for s in symbols]

    async def _run_forever(self) -> None:
        self._running = True
        assert self.bar_handler and self.quote_handler
        ts = Timestamp(seconds=int(T0.timestamp()) + 60, nanoseconds=0)  # msgpack, as on the wire
        await self.bar_handler(
            {"S": "BTC/USD", "t": ts, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 3}
        )
        await self.quote_handler({"S": "BTC/USD", "t": ts, "bp": 99.0, "ap": 101.0})

    async def stop_ws(self) -> None:
        self.stopped = True
        self._running = False


def source(hist: FakeHistorical | None = None, stream: FakeStream | None = None) -> AlpacaBarSource:
    return AlpacaBarSource(
        ["BTC/USD"], historical=hist or FakeHistorical(), stream=stream or FakeStream()
    )


async def test_history_returns_window_sorted_as_bars() -> None:
    bars = await source().history(["BTC/USD"], T0, T0 + timedelta(minutes=5))

    assert [b.ts for b in bars] == [T0, T0 + timedelta(minutes=1)]  # 10:05 excluded (end)
    assert bars[1] == Bar(
        symbol="BTC/USD",
        timeframe="1m",
        ts=T0 + timedelta(minutes=1),
        open=101,
        high=102,
        low=100,
        close=101,
        volume=0.5,
    )


async def test_history_failure_is_a_broker_error() -> None:
    with pytest.raises(BrokerError) as exc:
        await source(FakeHistorical(fail=True)).history(["BTC/USD"], T0, T0 + timedelta(hours=1))

    assert isinstance(exc.value.__cause__, ConnectionError)


async def test_latest_reports_bar_end_and_quote() -> None:
    latest = await source().latest("BTC/USD")

    assert latest.bar_end == T0 + timedelta(minutes=4)  # 10:03 bar ends at 10:04
    assert latest.quote == Quote(
        symbol="BTC/USD",
        ts=datetime(2026, 10, 11, 10, 4, 30, 500000, tzinfo=UTC),
        bid=99.9,
        ask=100.1,
    )


async def test_stream_converts_msgpack_payloads_and_tracks_connection() -> None:
    stream = FakeStream()
    src = source(stream=stream)
    bars: list[Bar] = []
    quotes: list[Quote] = []

    async def on_bar(b: Bar) -> None:
        bars.append(b)

    async def on_quote(q: Quote) -> None:
        quotes.append(q)

    assert not src.connected()
    await src.stream(on_bar, on_quote)

    assert stream.subscribed == ["bars:BTC/USD", "quotes:BTC/USD"]
    assert bars[0].ts == T0 + timedelta(minutes=1)
    assert (bars[0].close, bars[0].timeframe) == (1.5, "1m")
    assert (quotes[0].bid, quotes[0].ask) == (99.0, 101.0)
    assert src.connected()
    await src.stop()
    assert stream.stopped and not src.connected()


async def test_failing_callback_does_not_kill_the_stream() -> None:
    src = source()
    quotes: list[Quote] = []

    async def broken(_: Bar) -> None:
        raise RuntimeError("db down")

    async def on_quote(q: Quote) -> None:
        quotes.append(q)

    await src.stream(broken, on_quote)

    assert len(quotes) == 1  # the quote after the failing bar was still delivered


async def test_failing_quote_callback_is_contained() -> None:
    src = source()
    bars: list[Bar] = []

    async def on_bar(b: Bar) -> None:
        bars.append(b)

    async def broken(_: Quote) -> None:
        raise RuntimeError("boom")

    await src.stream(on_bar, broken)

    assert len(bars) == 1


def test_unknown_timestamp_type_is_rejected() -> None:
    from autotrader.broker.alpaca_data import _ts

    with pytest.raises(ValueError, match="timestamp"):
        _ts(12345)
