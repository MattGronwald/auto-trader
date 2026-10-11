"""Market data feed (SPEC §4.1, PLAN G5/G11/D6).

Persists 1m bars, resamples them to the profile's timeframes, keeps rolling in-memory
history and announces each closed bar as `BarClosed`.

Health is split in two, because Alpaca's crypto feed is sparse (minutes without any
bar, quotes only on book changes), so "no data" usually means "quiet market":
- **Feed health** is connection-level (`source.connected()`). Before the first connection
  the feed is starting up (no event; every symbol counts as stale). While disconnected nothing
  is closed by time (no flat bars fabricated while blind) and every symbol counts as
  stale; on reconnect the gap is backfilled first.
- **Price age** per symbol is the time since the last bar or quote. Past
  `stale_alert_s` the feed asks REST for the latest bar/quote (at most once per symbol
  per `stale_alert_s`) before declaring the symbol stale. If REST shows a bar the stream
  missed, that gap is backfilled.

Backfilled history warms the in-memory frames but publishes no events, so the scanner
never fires on old bars at startup. Hourly per-symbol bar counts are logged
(`feed.density`) to judge how usable each symbol is on this venue.
"""

from collections import Counter, deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import structlog
from pydantic import AwareDatetime
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.bus import DomainEvent, EventBus
from autotrader.core.features import MAX_INDICATOR_LOOKBACK
from autotrader.core.resample import Resampler, bucket_start, parse_timeframe
from autotrader.core.types import Bar, Quote
from autotrader.journal import repo

log = structlog.get_logger(__name__)

ONE_MINUTE = timedelta(minutes=1)


# --- events --------------------------------------------------------------------------


class BarClosed(DomainEvent):
    symbol: str
    timeframe: str
    bar_ts: AwareDatetime  # bar open time
    close: float


class SymbolStale(DomainEvent):
    symbol: str
    age_s: float


class SymbolFresh(DomainEvent):
    symbol: str


class FeedConnected(DomainEvent):
    pass


class FeedDisconnected(DomainEvent):
    pass


# --- source --------------------------------------------------------------------------


@dataclass(frozen=True)
class Latest:
    bar_end: datetime | None  # end (open + 1m) of the latest 1m bar REST knows
    quote_ts: datetime | None


class BarSource(Protocol):
    async def history(self, symbols: Sequence[str], start: datetime, end: datetime) -> list[Bar]:
        """1m bars with start <= ts < end, oldest first."""
        ...

    async def latest(self, symbol: str) -> Latest: ...

    async def stream(
        self,
        on_bar: Callable[[Bar], Awaitable[None]],
        on_quote: Callable[[Quote], Awaitable[None]],
    ) -> None:
        """Run until stopped, calling back for every 1m bar and quote."""
        ...

    def connected(self) -> bool: ...

    async def stop(self) -> None: ...


def backfill_depth(timeframes: Sequence[str], lookback: int = MAX_INDICATOR_LOOKBACK) -> timedelta:
    """G5: longest indicator lookback x largest timeframe, plus one bucket so the oldest
    bucket in the window is complete."""
    largest = max((parse_timeframe(tf) for tf in timeframes), default=ONE_MINUTE)
    return largest * (lookback + 1)


# --- feed ----------------------------------------------------------------------------


class MarketFeed:
    def __init__(
        self,
        source: BarSource,
        sessions: async_sessionmaker[AsyncSession],
        bus: EventBus,
        *,
        symbols: list[str],
        timeframes: list[str],
        stale_alert_s: int,
        lookback: int = MAX_INDICATOR_LOOKBACK,
        grace: timedelta = timedelta(seconds=10),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._source = source
        self._sessions = sessions
        self._bus = bus
        self._symbols = symbols
        self._timeframes = [tf for tf in timeframes if tf != "1m"]
        self._stale_after = timedelta(seconds=stale_alert_s)
        self._depth = backfill_depth(self._timeframes, lookback)
        self._clock = clock
        self._resampler = Resampler(self._timeframes, grace=grace)
        self._history: dict[tuple[str, str], deque[Bar]] = {
            (s, tf): deque(maxlen=2 * lookback) for s in symbols for tf in ["1m", *self._timeframes]
        }
        self._last_bar_ts: dict[str, datetime] = {}
        self._observed: dict[str, datetime] = {}  # latest bar end / quote time per symbol
        self._quotes: dict[str, Quote] = {}
        self._stale: set[str] = set()
        self._rest_checked: dict[str, datetime] = {}
        self._connected: bool | None = None
        self._density: Counter[str] = Counter()
        self._density_hour: datetime | None = None

    # --- queries ---------------------------------------------------------------------

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return list(self._history.get((symbol, timeframe), ()))

    def spread_bps(self, symbol: str) -> float | None:
        q = self._quotes.get(symbol)
        if q is None or q.bid <= 0 or q.ask <= 0:
            return None
        mid = (q.bid + q.ask) / 2
        return (q.ask - q.bid) / mid * 10_000

    def is_stale(self, symbol: str) -> bool:
        if self._connected is False or symbol not in self._observed:
            return True
        return symbol in self._stale

    # --- startup ---------------------------------------------------------------------

    async def backfill(self) -> None:
        """Fetch what the DB lacks for the G5 window, persist it, warm history. No events."""
        now = self._clock()
        end = bucket_start(now, ONE_MINUTE)  # exclude the still forming minute
        start = end - self._depth
        for symbol in self._symbols:
            async with self._sessions() as session:
                latest = await repo.latest_bar_ts(session, symbol, "1m")
            fetch_from = max(start, latest + ONE_MINUTE) if latest else start
            if fetch_from < end:
                bars = await self._source.history([symbol], fetch_from, end)
                async with self._sessions() as session:
                    await repo.upsert_bars(session, bars)
            async with self._sessions() as session:
                stored = await repo.get_bars(session, symbol, "1m", since=start, until=end)
            for b in stored:
                self._remember(b)
                for higher in self._resampler.add(b):
                    self._append(higher)
            log.info("feed.backfilled", symbol=symbol, bars=len(stored), since=start.isoformat())
        for higher in self._resampler.close_until(now):
            self._append(higher)

    # --- live input ------------------------------------------------------------------

    async def on_bar(self, bar: Bar) -> None:
        last = self._last_bar_ts.get(bar.symbol)
        if last is not None and bar.ts <= last:
            return  # duplicate or older (backfill overlap, reconnect replay)
        async with self._sessions() as session:
            await repo.upsert_bars(session, [bar])
        self._remember(bar)
        self._density[bar.symbol] += 1
        await self._announce(bar)
        for higher in self._resampler.add(bar):
            self._append(higher)
            await self._announce(higher)
        await self._mark_fresh(bar.symbol)

    async def on_quote(self, quote: Quote) -> None:
        self._quotes[quote.symbol] = quote
        self._observe(quote.symbol, quote.ts)
        await self._mark_fresh(quote.symbol)

    async def tick(self) -> None:
        """Run every ~second: connection state, time-based closes, staleness, density."""
        now = self._clock()
        connected = self._source.connected()
        if not connected and self._connected is None:
            return  # still starting up: never connected yet is not an outage
        if connected != self._connected:
            was = self._connected
            self._connected = connected
            await self._bus.publish(FeedConnected() if connected else FeedDisconnected())
            log.warning("feed.connected" if connected else "feed.disconnected")
            if connected and was is False:
                await self._fill_gaps(now)
        if not connected:
            return
        for higher in self._resampler.close_until(now):
            self._append(higher)
            await self._announce(higher)
        for symbol in self._symbols:
            await self._check_age(symbol, now)
        self._log_density(now)

    # --- internals -------------------------------------------------------------------

    def _remember(self, bar: Bar) -> None:
        self._append(bar)
        self._last_bar_ts[bar.symbol] = bar.ts
        self._observe(bar.symbol, bar.ts + ONE_MINUTE)

    def _append(self, bar: Bar) -> None:
        self._history.setdefault((bar.symbol, bar.timeframe), deque()).append(bar)

    def _observe(self, symbol: str, ts: datetime) -> None:
        if symbol not in self._observed or ts > self._observed[symbol]:
            self._observed[symbol] = ts

    async def _announce(self, bar: Bar) -> None:
        await self._bus.publish(
            BarClosed(symbol=bar.symbol, timeframe=bar.timeframe, bar_ts=bar.ts, close=bar.close)
        )

    async def _mark_fresh(self, symbol: str) -> None:
        if symbol in self._stale and not self._too_old(symbol, self._clock()):
            self._stale.discard(symbol)
            await self._bus.publish(SymbolFresh(symbol=symbol))
            log.info("feed.symbol_fresh", symbol=symbol)

    def _too_old(self, symbol: str, now: datetime) -> bool:
        seen = self._observed.get(symbol)
        return seen is None or now - seen > self._stale_after

    async def _check_age(self, symbol: str, now: datetime) -> None:
        if not self._too_old(symbol, now):
            return
        checked = self._rest_checked.get(symbol)
        if checked is None or now - checked >= self._stale_after:
            self._rest_checked[symbol] = now
            await self._refresh_from_rest(symbol, now)
            if not self._too_old(symbol, now):
                await self._mark_fresh(symbol)
                return
        if symbol not in self._stale:
            self._stale.add(symbol)
            seen = self._observed.get(symbol)
            age = (now - seen).total_seconds() if seen else float("inf")
            await self._bus.publish(SymbolStale(symbol=symbol, age_s=age))
            log.warning("feed.symbol_stale", symbol=symbol, age_s=age)

    async def _refresh_from_rest(self, symbol: str, now: datetime) -> None:
        try:
            latest = await self._source.latest(symbol)
        except Exception:
            log.exception("feed.rest_refresh_failed", symbol=symbol)
            return
        last = self._last_bar_ts.get(symbol)
        if latest.bar_end is not None and (last is None or latest.bar_end > last + ONE_MINUTE):
            await self._fill_gap(symbol, now)  # the stream missed at least one bar
        if latest.quote_ts is not None:
            self._observe(symbol, latest.quote_ts)

    async def _fill_gaps(self, now: datetime) -> None:
        for symbol in self._symbols:
            await self._fill_gap(symbol, now)

    async def _fill_gap(self, symbol: str, now: datetime) -> None:
        last = self._last_bar_ts.get(symbol)
        start = last + ONE_MINUTE if last else bucket_start(now, ONE_MINUTE) - self._depth
        end = bucket_start(now, ONE_MINUTE)
        if start >= end:
            return
        try:
            bars = await self._source.history([symbol], start, end)
        except Exception:
            log.exception("feed.gap_fill_failed", symbol=symbol)
            return
        for b in bars:
            await self.on_bar(b)
        log.info("feed.gap_filled", symbol=symbol, bars=len(bars), since=start.isoformat())

    def _log_density(self, now: datetime) -> None:
        hour = now.replace(minute=0, second=0, microsecond=0)
        if self._density_hour is None:
            self._density_hour = hour
        elif hour > self._density_hour:
            log.info(
                "feed.density",
                hour=self._density_hour.isoformat(),
                bars_per_symbol={s: self._density[s] for s in self._symbols},
            )
            self._density.clear()
            self._density_hour = hour
