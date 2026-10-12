"""Market data feed (SPEC §4.1, PLAN G5/G11/G13, D6/D11).

Persists 1m bars, resamples them to the profile's timeframes, keeps rolling in-memory
history and announces each closed bar as `BarClosed`.

**Verify before finalising.** Alpaca's crypto feed is sparse (minutes without any bar
are normal), so a missing minute in the stream can't be told apart from a minute
without trades. Stream bars are therefore only *pending*: persisted and announced as 1m
right away, but not resampled. When a window of the smallest higher timeframe is due
(its end + `grace` has passed), one REST fetch for all symbols covers it. REST is
authoritative: it adds minutes the stream missed and replaces minutes the stream sent
differently (persisted, logged as `feed.bar_corrected`, not re-announced). Accepted bars
from either source update price age, and stale symbols are marked fresh *before* the
recovered bars and aggregates are announced, so subscribers see the final state. The
window is then fed to the resampler in order.
Higher timeframes are built from these verified minutes, so they are complete too.
If REST keeps failing, the window is closed from stream data after `unverified_after`
and logged as `feed.unverified_close`. While disconnected nothing is finalised (no flat
bars made up while blind). A lock serialises stream input and finalising, so nothing
slips in while a REST request is in flight.

**Health is split (D11).** Feed health is the websocket connection; before the first
connection the feed is starting up (no event). Per symbol, price age is the time since
the last bar or quote; past `stale_alert_s` the feed asks REST for the latest bar/quote
(at most once per symbol per `stale_alert_s`) before publishing `SymbolStale`.

**Quotes are validated before use.** A quote from the stream or the REST refresh is
stored only if bid and ask are finite and > 0, bid <= ask (a locked book is fine) and it
is not older than the stored one. Anything else is dropped (`feed.quote_rejected`) and
does not refresh price age, so a feed sending garbage still goes stale. The REST refresh stores
the quote itself, not just its timestamp, so a symbol never turns fresh on old prices.

Backfill always refetches the full G5 window, so holes left in the DB by an earlier run
heal on restart; it warms history but publishes no events, so the scanner never fires on
old bars. Hourly per-symbol bar counts are logged (`feed.density`).
"""

import asyncio
import math
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
# Longest civil day (DST fall-back, e.g. Europe/Berlin 2026-10-25). Sizing on 24 h would
# drop a non-UTC trading day's start from the window during its last hour.
LONGEST_DAY = timedelta(hours=25)


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
    quote: Quote | None


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
    bucket in the window is complete. At least the longest civil day plus a bucket, so the
    day-anchored VWAP (D12) has the current trading day's start in the window."""
    largest = max((parse_timeframe(tf) for tf in timeframes), default=ONE_MINUTE)
    return max(largest * (lookback + 1), LONGEST_DAY + largest)


def history_size(timeframe: str, lookback: int = MAX_INDICATOR_LOOKBACK) -> int:
    """Bars kept in memory per symbol/timeframe: 2 x lookback for the indicators, but at
    least the longest civil day plus one bar so the VWAP day anchor (D12) stays in the
    window, also on a 25 h DST day in a non-UTC `trading_day_tz`."""
    return max(2 * lookback, LONGEST_DAY // parse_timeframe(timeframe) + 1)


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
        grace: timedelta = timedelta(seconds=20),
        unverified_after: timedelta = timedelta(seconds=60),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._source = source
        self._sessions = sessions
        self._bus = bus
        self._symbols = symbols
        self._timeframes = [tf for tf in timeframes if tf != "1m"]
        self._window = min((parse_timeframe(tf) for tf in self._timeframes), default=ONE_MINUTE)
        self._stale_after = timedelta(seconds=stale_alert_s)
        self._depth = backfill_depth(self._timeframes, lookback)
        self._grace = grace
        self._unverified_after = unverified_after
        self._clock = clock
        self._resampler = Resampler(self._timeframes, grace=grace)
        self._history: dict[tuple[str, str], deque[Bar]] = {
            (s, tf): deque(maxlen=history_size(tf, lookback))
            for s in symbols
            for tf in ["1m", *self._timeframes]
        }
        self._lock = asyncio.Lock()
        self._pending: dict[str, dict[datetime, Bar]] = {s: {} for s in symbols}
        self._finalized_until = bucket_start(clock(), self._window)
        self._observed: dict[str, datetime] = {}  # latest bar end / quote time per symbol
        self._quotes: dict[str, Quote] = {}
        self._stale: set[str] = set()
        self._rest_checked: dict[str, datetime] = {}
        self._connected: bool | None = None
        self.late_bars = 0
        self._density: Counter[str] = Counter()
        self._density_hour: datetime | None = None

    # --- queries ---------------------------------------------------------------------

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return list(self._history.get((symbol, timeframe), ()))

    def quote(self, symbol: str) -> Quote | None:
        return self._quotes.get(symbol)

    def last_price(self, symbol: str) -> float | None:
        """Close of the newest 1m bar, pending (unverified) or verified: the freshest
        trade price the feed has. Used for marking and price sanity, not for signals."""
        candidates = list(self._pending.get(symbol, {}).values())
        history = self._history.get((symbol, "1m"))
        if history:
            candidates.append(history[-1])
        return max(candidates, key=lambda b: b.ts).close if candidates else None

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
        """Refetch the full G5 window, persist it, warm history. Publishes no events."""
        now = self._clock()
        end = bucket_start(now, ONE_MINUTE)  # exclude the still forming minute
        start = end - self._depth
        # Same rule as tick(): a window is only final once its end + grace has passed.
        self._finalized_until = bucket_start(now - self._grace, self._window)
        fetched = await self._source.history(self._symbols, start, end)
        async with self._sessions() as session:
            await repo.upsert_bars(session, fetched)
        for symbol in self._symbols:
            async with self._sessions() as session:
                stored = await repo.get_bars(session, symbol, "1m", since=start, until=end)
            for b in stored:
                self._observe(symbol, b.ts + ONE_MINUTE)
                if b.ts >= self._finalized_until:
                    self._pending[symbol][b.ts] = b  # current window: verified on finalise
                    continue
                self._append(b)
                for higher in self._resampler.add(b):
                    self._append(higher)
            log.info("feed.backfilled", symbol=symbol, bars=len(stored), since=start.isoformat())
        for higher in self._resampler.close_until(self._finalized_until + self._grace):
            self._append(higher)

    # --- live input ------------------------------------------------------------------

    async def on_bar(self, bar: Bar) -> None:
        async with self._lock:
            self._observe(bar.symbol, bar.ts + ONE_MINUTE)
            if bar.ts < self._finalized_until:
                # Arrived after its window was verified and finalised: keep the DB right,
                # but the aggregates are already published.
                self.late_bars += 1
                async with self._sessions() as session:
                    await repo.upsert_bars(session, [bar])
                log.warning("feed.late_bar", symbol=bar.symbol, ts=bar.ts.isoformat())
            else:
                new = await self._accept(bar, announce=False)
                # Freshness before the announcement, as in finalising: subscribers must
                # never see a fresh bar while the symbol still reads as stale.
                await self._mark_fresh(bar.symbol)
                if new:
                    await self._announce(bar)
                return
        await self._mark_fresh(bar.symbol)

    async def on_quote(self, quote: Quote) -> None:
        if self._accept_quote(quote, source="stream"):
            await self._mark_fresh(quote.symbol)

    async def tick(self) -> None:
        """Run every ~second: connection state, verified finalising, staleness, density."""
        now = self._clock()
        connected = self._source.connected()
        if not connected and self._connected is None:
            return  # still starting up: never connected yet is not an outage
        if connected != self._connected:
            self._connected = connected
            await self._bus.publish(FeedConnected() if connected else FeedDisconnected())
            log.warning("feed.connected" if connected else "feed.disconnected")
        if not connected:
            return
        await self._finalize_due(now)
        for symbol in self._symbols:
            await self._check_age(symbol, now)
        self._log_density(now)

    # --- verified finalising ---------------------------------------------------------

    async def _finalize_due(self, now: datetime) -> None:
        due_end = bucket_start(now - self._grace, self._window)
        if due_end <= self._finalized_until:
            return
        async with self._lock:
            start = self._finalized_until
            try:
                verified = await self._source.history(self._symbols, start, due_end)
            except Exception:
                if now - (due_end + self._grace) < self._unverified_after:
                    log.warning("feed.verify_failed_retrying", since=start.isoformat())
                    return  # retry on the next tick
                log.exception(
                    "feed.unverified_close", since=start.isoformat(), until=due_end.isoformat()
                )
                verified = []
            # 1. Merge: REST is authoritative within verification. It adds minutes the
            #    stream missed and corrects minutes the stream delivered differently.
            #    Persisted and price age updated, but nothing announced yet.
            recovered: list[Bar] = []
            for b in verified:
                old = self._pending[b.symbol].get(b.ts)
                if old is not None and old != b:
                    log.info("feed.bar_corrected", symbol=b.symbol, ts=b.ts.isoformat())
                if old != b and await self._accept(b, announce=False):
                    recovered.append(b)
            # 2. Freshness first, so subscribers of the bars below see the final state.
            for symbol in self._symbols:
                await self._mark_fresh(symbol)
            # 3. Announce recovered minutes, then the verified aggregates.
            for b in recovered:
                await self._announce(b)
            for symbol in self._symbols:
                for ts in sorted(t for t in self._pending[symbol] if t < due_end):
                    b = self._pending[symbol].pop(ts)
                    self._append(b)
                    for higher in self._resampler.add(b):
                        self._append(higher)
                        await self._announce(higher)
            for higher in self._resampler.close_until(due_end + self._grace):
                self._append(higher)
                await self._announce(higher)
            self._finalized_until = due_end

    # --- internals -------------------------------------------------------------------

    async def _accept(self, bar: Bar, *, announce: bool = True) -> bool:
        """New, duplicate or corrected bar for an unfinalised window: keep the latest
        version, persist it, update price age. Returns whether the minute is new; it is
        announced (if `announce`) only the first time."""
        new = bar.ts not in self._pending[bar.symbol]
        self._pending[bar.symbol][bar.ts] = bar
        self._observe(bar.symbol, bar.ts + ONE_MINUTE)
        async with self._sessions() as session:
            await repo.upsert_bars(session, [bar])
        if new:
            self._density[bar.symbol] += 1
            if announce:
                await self._announce(bar)
        return new

    def _append(self, bar: Bar) -> None:
        self._history.setdefault((bar.symbol, bar.timeframe), deque()).append(bar)

    def _accept_quote(self, quote: Quote, *, source: str) -> bool:
        """Store `quote` if it is a usable, newer top of book; the only way in."""
        stored = self._quotes.get(quote.symbol)
        # First: NaN fails every comparison below and (inf, inf) looks like a locked book.
        if not (math.isfinite(quote.bid) and math.isfinite(quote.ask)):
            reason = "non-finite"
        elif quote.bid <= 0 or quote.ask <= 0:
            reason = "non-positive"
        elif quote.bid > quote.ask:
            reason = "crossed"
        elif stored is not None and quote.ts < stored.ts:
            reason = "out of order"
        else:
            self._quotes[quote.symbol] = quote
            self._observe(quote.symbol, quote.ts)
            return True
        log.info(
            "feed.quote_rejected",
            symbol=quote.symbol,
            source=source,
            reason=reason,
            bid=quote.bid,
            ask=quote.ask,
            ts=quote.ts.isoformat(),
        )
        return False

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
            await self._refresh_from_rest(symbol)
            if not self._too_old(symbol, now):
                await self._mark_fresh(symbol)
                return
        if symbol not in self._stale:
            self._stale.add(symbol)
            seen = self._observed.get(symbol)
            age = (now - seen).total_seconds() if seen else float("inf")
            await self._bus.publish(SymbolStale(symbol=symbol, age_s=age))
            log.warning("feed.symbol_stale", symbol=symbol, age_s=age)

    async def _refresh_from_rest(self, symbol: str) -> None:
        """Price freshness only; missed bars are recovered when their window finalises."""
        try:
            latest = await self._source.latest(symbol)
        except Exception:
            log.exception("feed.rest_refresh_failed", symbol=symbol)
            return
        if latest.bar_end is not None:
            self._observe(symbol, latest.bar_end)
        if latest.quote is not None:
            self._accept_quote(latest.quote, source="rest")

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
