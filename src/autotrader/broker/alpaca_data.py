"""Alpaca crypto market data as a `BarSource` for `core.feed.MarketFeed`.

Shapes verified against the live API on 2026-10-11 (docs/alpaca-notes.md):
- REST (`raw_data=True`): `{symbol: [{"t": "...Z", "o", "h", "l", "c", "v", ...}]}`.
- Stream: msgpack payloads with `"S"` (symbol) and `"t"` as a msgpack `Timestamp`.
- The feed is sparse: many minutes have no bar, quotes arrive only on book changes.

The stream runs inside our event loop via `CryptoDataStream._run_forever()`, a private
alpaca-py method (the public `run()` starts its own loop); it reconnects on its own.
`connected()` reads its private `_running` flag. Both are pinned by tests against a fake
and a smoke test against the real stream; re-check on alpaca-py upgrades.
"""

from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import structlog

from autotrader.broker.alpaca import _call
from autotrader.core.config import Settings
from autotrader.core.feed import Latest
from autotrader.core.types import Bar, Quote

log = structlog.get_logger(__name__)

ONE_MINUTE = timedelta(minutes=1)


class _Historical(Protocol):
    def get_crypto_bars(self, request: Any) -> Any: ...
    def get_crypto_latest_bar(self, request: Any) -> Any: ...
    def get_crypto_latest_quote(self, request: Any) -> Any: ...


class _Stream(Protocol):
    def subscribe_bars(self, handler: Callable[[Any], Awaitable[None]], *symbols: str) -> None: ...
    def subscribe_quotes(
        self, handler: Callable[[Any], Awaitable[None]], *symbols: str
    ) -> None: ...
    async def _run_forever(self) -> None: ...
    async def stop_ws(self) -> None: ...


def _quote(symbol: str, raw: dict[str, Any]) -> Quote:
    """Stream and REST share `t`, `bp`, `ap`; REST keys each quote by symbol instead of `S`."""
    return Quote(symbol=symbol, ts=_ts(raw["t"]), bid=raw["bp"], ask=raw["ap"])


def _ts(value: Any) -> datetime:
    """REST sends ISO strings, the stream msgpack Timestamps."""
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    if hasattr(value, "to_datetime"):
        dt: datetime = value.to_datetime()
        return dt.astimezone(UTC)
    raise ValueError(f"unexpected timestamp {value!r}")


def _bar(symbol: str, raw: dict[str, Any]) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe="1m",
        ts=_ts(raw["t"]),
        open=raw["o"],
        high=raw["h"],
        low=raw["l"],
        close=raw["c"],
        volume=raw["v"],
    )


class AlpacaBarSource:
    def __init__(self, symbols: Sequence[str], *, historical: _Historical, stream: _Stream) -> None:
        self._symbols = list(symbols)
        self._historical = historical
        self._stream = stream

    @classmethod
    def from_settings(cls, settings: Settings, symbols: Sequence[str]) -> "AlpacaBarSource":
        from alpaca.data.historical.crypto import CryptoHistoricalDataClient
        from alpaca.data.live.crypto import CryptoDataStream

        key = settings.alpaca_api_key_id.get_secret_value() if settings.alpaca_api_key_id else None
        secret = (
            settings.alpaca_api_secret_key.get_secret_value()
            if settings.alpaca_api_secret_key
            else None
        )
        if key is None or secret is None:
            from autotrader.broker.base import BrokerError

            raise BrokerError("ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY are not set")
        historical: _Historical = CryptoHistoricalDataClient(key, secret, raw_data=True)
        stream: _Stream = CryptoDataStream(key, secret, raw_data=True)
        return cls(symbols, historical=historical, stream=stream)

    async def history(self, symbols: Sequence[str], start: datetime, end: datetime) -> list[Bar]:
        from alpaca.data.requests import CryptoBarsRequest
        from alpaca.data.timeframe import TimeFrame

        request = CryptoBarsRequest(
            symbol_or_symbols=list(symbols), timeframe=TimeFrame.Minute, start=start, end=end
        )
        raw = await _call("get_crypto_bars", self._historical.get_crypto_bars, request)
        bars = [_bar(sym, row) for sym, rows in raw.items() for row in rows]
        return sorted((b for b in bars if start <= b.ts < end), key=lambda b: (b.ts, b.symbol))

    async def latest(self, symbol: str) -> Latest:
        from alpaca.data.requests import (
            CryptoLatestBarRequest,
            CryptoLatestQuoteRequest,
        )

        bar_raw = await _call(
            "get_crypto_latest_bar",
            self._historical.get_crypto_latest_bar,
            CryptoLatestBarRequest(symbol_or_symbols=symbol),
        )
        quote_raw = await _call(
            "get_crypto_latest_quote",
            self._historical.get_crypto_latest_quote,
            CryptoLatestQuoteRequest(symbol_or_symbols=symbol),
        )
        bar = bar_raw.get(symbol)
        quote = quote_raw.get(symbol)
        return Latest(
            bar_end=_ts(bar["t"]) + ONE_MINUTE if bar else None,
            quote=_quote(symbol, quote) if quote else None,
        )

    async def stream(
        self,
        on_bar: Callable[[Bar], Awaitable[None]],
        on_quote: Callable[[Quote], Awaitable[None]],
    ) -> None:
        async def handle_bar(raw: dict[str, Any]) -> None:
            try:
                await on_bar(_bar(raw["S"], raw))
            except Exception:  # a failing consumer must not kill the receive loop
                log.exception("alpaca_stream.bar_handler_failed", symbol=raw.get("S"))

        async def handle_quote(raw: dict[str, Any]) -> None:
            try:
                await on_quote(_quote(raw["S"], raw))
            except Exception:
                log.exception("alpaca_stream.quote_handler_failed", symbol=raw.get("S"))

        self._stream.subscribe_bars(handle_bar, *self._symbols)
        self._stream.subscribe_quotes(handle_quote, *self._symbols)
        await self._stream._run_forever()

    def connected(self) -> bool:
        return bool(getattr(self._stream, "_running", False))

    async def stop(self) -> None:
        await self._stream.stop_ws()
