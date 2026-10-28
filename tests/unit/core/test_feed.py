import asyncio
import io
import json
from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.core.bus import EventBus
from autotrader.core.feed import (
    BarClosed,
    FeedConnected,
    FeedDisconnected,
    Latest,
    MarketFeed,
    SymbolFresh,
    SymbolStale,
    backfill_depth,
    history_size,
)
from autotrader.core.indicators import compute_features
from autotrader.core.logging import configure_logging
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
M = timedelta(minutes=1)
S = timedelta(seconds=1)
GRACE = 20 * S
BTC = "BTC/USD"


def bar(minute: int, price: float = 100.0, symbol: str = BTC) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe="1m",
        ts=T0 + minute * M,
        open=price,
        high=price + 1,
        low=price - 1,
        close=price,
        volume=1.0,
    )


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class FakeSource:
    """REST serves `bars` by window; `gate` (if set) pauses history() mid-request."""

    def __init__(self, bars: Sequence[Bar] = ()) -> None:
        self.bars = list(bars)
        self.history_calls: list[tuple[tuple[str, ...], datetime, datetime]] = []
        self.latest_bar: dict[str, Bar] = {}
        self.latest_quote: dict[str, Quote] = {}
        self.latest_calls = 0
        self.is_connected = True
        self.fail_history = False
        self.gate: asyncio.Event | None = None
        self.entered = asyncio.Event()

    async def history(self, symbols: Sequence[str], start: datetime, end: datetime) -> list[Bar]:
        self.history_calls.append((tuple(symbols), start, end))
        if self.fail_history:
            raise ConnectionError("rest down")
        result = [b for b in self.bars if b.symbol in symbols and start <= b.ts < end]
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        return result

    async def latest(self, symbol: str) -> Latest:
        self.latest_calls += 1
        return Latest(bar=self.latest_bar.get(symbol), quote=self.latest_quote.get(symbol))

    async def stream(
        self, on_bar: Callable[[Bar], Awaitable[None]], on_quote: Callable[[Quote], Awaitable[None]]
    ) -> None:
        return None

    def connected(self) -> bool:
        return self.is_connected

    async def stop(self) -> None:
        return None


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)  # sync: Alembic runs its own event loop
    return url


@pytest.fixture
async def sessions(db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = make_engine(db_url)
    yield make_sessionmaker(engine)
    await engine.dispose()


@pytest.fixture
async def bus(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[EventBus]:
    b = EventBus(sessions)
    yield b
    await b.close()


@pytest.fixture
async def announced(bus: EventBus) -> list[BarClosed]:  # async: subscribe starts a task
    events: list[BarClosed] = []

    async def on_closed(e: BarClosed) -> None:
        events.append(e)

    bus.subscribe(BarClosed, on_closed)
    return events


def make_feed(
    source: FakeSource,
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    clock: Clock,
    symbols: Sequence[str] = (BTC,),
) -> MarketFeed:
    return MarketFeed(
        source,
        sessions,
        bus,
        symbols=list(symbols),
        timeframes=["5m", "1h"],
        stale_alert_s=60,
        lookback=10,
        clock=clock,
    )


async def event_types(sessions: async_sessionmaker[AsyncSession]) -> list[str]:
    async with sessions() as s:
        return [e.type for e in await s.scalars(select(m.Event).order_by(m.Event.id))]


async def bar_count(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as s:
        return int(await s.scalar(select(func.count()).select_from(m.BarRow)) or 0)


def test_backfill_depth_is_lookback_times_largest_timeframe_plus_one_bucket() -> None:
    # PLAN G5: 50 x 4h ~ 9 days; one extra bucket so the oldest one is complete.
    assert backfill_depth(["5m", "15m", "1h", "4h"], lookback=50) == timedelta(hours=4 * 51)


def test_backfill_depth_covers_at_least_one_day_for_vwap() -> None:
    # D12: VWAP is anchored at the trading-day start, so the window must reach back past it.
    # 25 h: the longest civil day (DST fall-back), so a non-UTC trading day still fits.
    assert backfill_depth(["5m"], lookback=50) == timedelta(hours=25, minutes=5)
    assert backfill_depth(["1m"], lookback=50) == timedelta(hours=25, minutes=1)


async def test_history_retains_a_full_day_per_timeframe(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # D12: with only 2 x lookback bars, 1m VWAP would lose its day anchor after minutes.
    clock = Clock(T0 + 1600 * M + GRACE)
    feed = make_feed(FakeSource([bar(i) for i in range(1600)]), sessions, bus, clock)

    await feed.backfill()

    assert len(feed.history(BTC, "1m")) == 25 * 60 + 1
    assert len(feed.history(BTC, "5m")) == 25 * 12 + 1
    assert len(feed.history(BTC, "1h")) == 25 + 1


def test_retained_history_keeps_vwap_anchor_on_a_25_hour_dst_day() -> None:
    # PR #11 review: Europe/Berlin's 2026-10-25 lasts 25 h. Near its end the retained
    # 1m window must still reach back past the local day start, or VWAP masks to NaN.
    start = datetime(2026, 10, 24, 21, 59, tzinfo=UTC)  # 23:59 Berlin on the 24th
    bars = [
        Bar(
            symbol=BTC,
            timeframe="1m",
            ts=start + i * M,
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=1.0,
        )
        for i in range(25 * 60 + 1)  # through 22:59 UTC = 23:59 Berlin on the 25th
    ]
    kept = bars[-history_size("1m") :]
    assert compute_features(kept, trading_day_tz="Europe/Berlin")["vwap"][-1] == 100.0


# --- backfill ------------------------------------------------------------------------


async def test_backfill_persists_and_warms_history_without_events(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + 30 * M + GRACE)
    source = FakeSource([bar(i, 100 + i) for i in range(30)])
    feed = make_feed(source, sessions, bus, clock)

    await feed.backfill()
    await bus.drain()

    assert await bar_count(sessions) == 30
    assert [b.close for b in feed.history(BTC, "1m")][-3:] == [127, 128, 129]
    assert [b.ts for b in feed.history(BTC, "5m")] == [T0 + i * 5 * M for i in range(6)]
    assert await event_types(sessions) == []  # old bars must not trigger the scanner


async def test_backfill_refetches_the_full_window_so_db_holes_heal(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # PR #10 review: fetching only after the DB maximum never repaired earlier holes.
    async with sessions() as s:
        await repo.upsert_bars(s, [bar(0), bar(9)])  # hole at minutes 1..8
    clock = Clock(T0 + 30 * M)
    source = FakeSource([bar(i) for i in range(30)])
    feed = make_feed(source, sessions, bus, clock)

    await feed.backfill()

    (_, start, end) = source.history_calls[0]
    assert end == T0 + 30 * M  # the still forming minute is excluded
    assert start == end - backfill_depth(["5m", "1h"], lookback=10)
    assert await bar_count(sessions) == 30


async def test_backfilled_current_window_is_finalised_later(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, announced: list[BarClosed]
) -> None:
    clock = Clock(T0 + 32 * M)
    source = FakeSource([bar(i) for i in range(32)])
    feed = make_feed(source, sessions, bus, clock)
    await feed.backfill()

    clock.now = T0 + 35 * M + GRACE
    await feed.tick()
    await bus.drain()

    assert [(e.timeframe, e.bar_ts) for e in announced] == [("5m", T0 + 30 * M)]


# --- live bars -----------------------------------------------------------------------


async def test_live_bar_is_persisted_and_announced_once(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, announced: list[BarClosed]
) -> None:
    feed = make_feed(FakeSource(), sessions, bus, Clock(T0 + M + 3 * S))

    await feed.on_bar(bar(0))
    await feed.on_bar(bar(0))  # duplicate (backfill overlap / reconnect)
    await bus.drain()

    assert await bar_count(sessions) == 1
    assert [(e.timeframe, e.bar_ts) for e in announced] == [("1m", T0)]


async def test_window_is_finalised_only_after_grace_and_verification(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, announced: list[BarClosed]
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource([bar(1, 100)])
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(1, 100))  # minutes 0, 2, 3, 4 have no bar (sparse feed)

    clock.now = T0 + 5 * M + GRACE - S
    await feed.tick()
    assert feed.history(BTC, "5m") == []  # still within grace
    clock.now += S
    await feed.tick()
    await bus.drain()

    assert source.history_calls[-1][1:] == (T0, T0 + 5 * M)  # one REST check for the window
    assert [(e.timeframe, e.bar_ts) for e in announced] == [("1m", T0 + M), ("5m", T0)]
    assert feed.history(BTC, "5m")[-1].close == 100


async def test_bar_missed_by_the_stream_is_in_the_aggregate(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, announced: list[BarClosed]
) -> None:
    # PR #10 review #2: the 5m bar used to close (close 100, vol 1) before REST recovered
    # minute 4 (price 200), and the recovered bar was then ignored as late.
    clock = Clock(T0 + M)
    source = FakeSource([bar(0, 100), bar(4, 200)])
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0, 100))

    clock.now = T0 + 5 * M + GRACE
    await feed.tick()
    await bus.drain()

    (five,) = feed.history(BTC, "5m")
    assert (five.open, five.high, five.close, five.volume) == (100, 201, 200, 2.0)
    five_events = [e for e in announced if e.timeframe == "5m"]
    assert [(e.bar_ts, e.close) for e in five_events] == [(T0, 200)]
    assert await bar_count(sessions) == 2


async def test_live_bar_before_the_reconnect_tick_keeps_the_gap(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # PR #10 review #1: a new live bar arrived before tick() noticed the reconnect, moved
    # the watermark, and minutes 2 and 4 were never fetched.
    clock = Clock(T0 + M)
    source = FakeSource([bar(0, 100), bar(2, 102), bar(4, 104)])
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0, 100))
    await feed.tick()  # connected

    source.is_connected = False
    clock.now = T0 + 3 * M
    await feed.tick()
    source.is_connected = True
    await feed.on_bar(bar(5, 105))  # the stream is back before the next tick
    clock.now = T0 + 5 * M + GRACE
    await feed.tick()
    await bus.drain()

    (five,) = feed.history(BTC, "5m")
    assert (five.high, five.close) == (105, 104)  # minutes 0, 2, 4 - not a partial bucket
    assert await bar_count(sessions) == 4
    assert (await event_types(sessions)).count("FeedDisconnected") == 1


async def test_live_bar_during_the_verification_fetch_is_not_dropped(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # PR #10 review #1, second case: live input while a recovery REST call was in flight.
    clock = Clock(T0 + M)
    source = FakeSource([bar(0, 100), bar(3, 103)])
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0, 100))
    await feed.tick()

    source.gate = asyncio.Event()
    source.entered.clear()
    clock.now = T0 + 5 * M + GRACE
    ticking = asyncio.create_task(feed.tick())
    await source.entered.wait()
    live = asyncio.create_task(feed.on_bar(bar(6, 106)))  # arrives mid-fetch
    for _ in range(10):
        await asyncio.sleep(0)
    source.gate.set()
    await asyncio.gather(ticking, live)

    (five,) = feed.history(BTC, "5m")
    assert five.close == 103  # REST-recovered minute 3 included
    assert await bar_count(sessions) == 3  # and live minute 6 persisted, not dropped
    source.gate = None
    clock.now = T0 + 10 * M + GRACE
    await feed.tick()
    assert feed.history(BTC, "5m")[-1].close == 106


async def test_rest_failure_retries_then_closes_unverified(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0, 100))
    source.fail_history = True

    clock.now = T0 + 5 * M + GRACE
    await feed.tick()
    assert feed.history(BTC, "5m") == []  # retrying
    clock.now += 61 * S
    await feed.tick()

    (five,) = feed.history(BTC, "5m")
    assert five.close == 100  # closed from stream data after unverified_after


async def test_bar_after_its_window_was_finalised_is_persisted_not_resampled(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource([bar(0)]), sessions, bus, clock)
    await feed.on_bar(bar(0))
    clock.now = T0 + 5 * M + GRACE
    await feed.tick()

    await feed.on_bar(bar(3, 999))

    assert feed.late_bars == 1
    assert await bar_count(sessions) == 2
    assert feed.history(BTC, "5m")[-1].close == 100


async def test_nothing_is_finalised_while_disconnected(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource([bar(0)])
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))
    await feed.tick()

    source.is_connected = False
    clock.now = T0 + 30 * M
    await feed.tick()

    assert feed.history(BTC, "5m") == []
    assert feed.is_stale(BTC)


async def test_not_yet_connected_at_startup_is_not_an_outage(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0)
    source = FakeSource()
    source.is_connected = False
    feed = make_feed(source, sessions, bus, clock)

    await feed.tick()
    source.is_connected = True
    await feed.tick()
    await bus.drain()

    types = await event_types(sessions)
    assert types[0] == "FeedConnected"
    assert "FeedDisconnected" not in types


# --- quotes / staleness --------------------------------------------------------------


async def test_quote_updates_spread(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    feed = make_feed(FakeSource(), sessions, bus, Clock(T0))

    await feed.on_quote(Quote(symbol=BTC, ts=T0, bid=99.95, ask=100.05))

    assert feed.spread_bps(BTC) == pytest.approx(10.0)
    assert feed.spread_bps("ETH/USD") is None


@pytest.mark.parametrize(
    ("bid", "ask"),
    [(100.05, 99.95), (0.0, 100.0), (99.0, 0.0), (-1.0, 100.0), (99.0, -1.0)],
    ids=["crossed", "zero-bid", "zero-ask", "negative-bid", "negative-ask"],
)
async def test_invalid_quote_is_dropped_and_does_not_refresh(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, bid: float, ask: float
) -> None:
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource(), sessions, bus, clock)
    good = Quote(symbol=BTC, ts=T0, bid=99.95, ask=100.05)
    await feed.on_quote(good)

    await feed.on_quote(Quote(symbol=BTC, ts=T0 + 30 * S, bid=bid, ask=ask))

    assert feed.quote(BTC) == good
    clock.now = T0 + 61 * S  # 61 s after the last *valid* quote
    await feed.tick()
    clock.now += S
    await feed.tick()
    assert feed.is_stale(BTC)


NAN, INF = float("nan"), float("inf")
NON_FINITE = pytest.mark.parametrize(
    ("bid", "ask"),
    [(NAN, 100.0), (99.0, NAN), (NAN, NAN), (INF, INF), (99.0, INF)],
    ids=["nan-bid", "nan-ask", "nan-both", "inf-locked", "inf-ask"],
)


@NON_FINITE
async def test_non_finite_stream_quote_is_dropped_and_does_not_refresh(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, bid: float, ask: float
) -> None:
    # NaN fails every comparison and (inf, inf) passes as a locked book, so both slipped
    # past the positive/crossed checks, replaced the valid book and kept the symbol fresh.
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource(), sessions, bus, clock)
    good = Quote(symbol=BTC, ts=T0, bid=99.95, ask=100.05)
    await feed.on_quote(good)

    await feed.on_quote(Quote(symbol=BTC, ts=T0 + 30 * S, bid=bid, ask=ask))

    assert feed.quote(BTC) == good
    assert feed.spread_bps(BTC) == pytest.approx(10.0)
    clock.now = T0 + 61 * S  # 61 s after the last *valid* quote
    await feed.tick()
    clock.now += S
    await feed.tick()
    assert feed.is_stale(BTC)


@NON_FINITE
async def test_non_finite_rest_quote_is_dropped_and_leaves_the_symbol_stale(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, bid: float, ask: float
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    good = Quote(symbol=BTC, ts=T0, bid=99.95, ask=100.05)
    await feed.on_quote(good)

    clock.now = T0 + 90 * S
    source.latest_quote[BTC] = Quote(symbol=BTC, ts=clock.now - 5 * S, bid=bid, ask=ask)
    await feed.tick()

    assert feed.is_stale(BTC)
    assert feed.quote(BTC) == good


async def test_locked_quote_is_accepted(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    feed = make_feed(FakeSource(), sessions, bus, Clock(T0))
    locked = Quote(symbol=BTC, ts=T0, bid=100.0, ask=100.0)

    await feed.on_quote(locked)

    assert feed.quote(BTC) == locked
    assert feed.spread_bps(BTC) == 0.0


async def test_older_stream_quote_does_not_replace_a_newer_one(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    feed = make_feed(FakeSource(), sessions, bus, Clock(T0))
    newer = Quote(symbol=BTC, ts=T0 + S, bid=101.0, ask=101.1)
    await feed.on_quote(newer)

    await feed.on_quote(Quote(symbol=BTC, ts=T0, bid=99.0, ask=99.1))

    assert feed.quote(BTC) == newer


async def test_rest_refresh_stores_the_quote_values(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # The REST refresh only recorded timestamps: a stale symbol turned fresh while the
    # stored bid/ask stayed at their old values, which check() then sized against.
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_quote(Quote(symbol=BTC, ts=T0 + M, bid=90.0, ask=90.1))
    clock.now = T0 + M + 62 * S
    await feed.tick()
    assert feed.is_stale(BTC)

    clock.now += 61 * S  # past the REST throttle
    rest = Quote(symbol=BTC, ts=clock.now - 2 * S, bid=99.0, ask=99.1)
    source.latest_quote[BTC] = rest
    await feed.tick()

    assert not feed.is_stale(BTC)
    assert feed.quote(BTC) == rest


async def test_older_rest_quote_does_not_replace_a_newer_stream_quote(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))
    stream = Quote(symbol=BTC, ts=T0 + 30 * S, bid=101.0, ask=101.1)
    await feed.on_quote(stream)

    clock.now = T0 + 30 * S + 61 * S
    source.latest_quote[BTC] = Quote(symbol=BTC, ts=T0 + 10 * S, bid=99.0, ask=99.1)
    await feed.tick()

    assert feed.quote(BTC) == stream


async def test_invalid_rest_quote_leaves_the_symbol_stale(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + M + 90 * S
    source.latest_quote[BTC] = Quote(symbol=BTC, ts=clock.now - 5 * S, bid=99.2, ask=99.1)
    await feed.tick()

    assert feed.is_stale(BTC)
    assert feed.quote(BTC) is None


async def test_symbol_goes_stale_once_and_recovers(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource(), sessions, bus, clock)
    await feed.on_bar(bar(0))
    assert not feed.is_stale(BTC)

    clock.now = T0 + M + 61 * S
    await feed.tick()
    clock.now += S
    await feed.tick()
    assert feed.is_stale(BTC)

    await feed.on_bar(bar(2))
    await bus.drain()

    assert not feed.is_stale(BTC)
    types = await event_types(sessions)
    assert types.count("SymbolStale") == 1
    assert "SymbolFresh" in types


async def test_never_seen_symbol_is_stale(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    assert make_feed(FakeSource(), sessions, bus, Clock(T0)).is_stale("ETH/USD")


async def test_rest_refresh_prevents_false_stale_on_a_quiet_market(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + M + 90 * S
    source.latest_quote[BTC] = Quote(symbol=BTC, ts=clock.now - 5 * S, bid=99.0, ask=99.1)
    await feed.tick()

    assert not feed.is_stale(BTC)
    assert "SymbolStale" not in await event_types(sessions)


async def test_rest_refresh_is_throttled(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + M + 61 * S
    for _ in range(30):  # 30 ticks within one stale window
        await feed.tick()
        clock.now += S

    assert source.latest_calls == 1


class BrokenLatest(FakeSource):
    async def latest(self, symbol: str) -> Latest:
        raise ConnectionError("rest down")


async def test_rest_refresh_failure_is_logged_not_raised(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    feed = make_feed(BrokenLatest(), sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + M + 120 * S
    await feed.tick()

    assert feed.is_stale(BTC)


async def test_density_is_logged_per_hour(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource(), sessions, bus, clock)
    await feed.tick()
    for i in range(3):
        await feed.on_bar(bar(i))

    clock.now = T0 + timedelta(hours=1, seconds=5)
    await feed.tick()

    lines = [json.loads(line) for line in buf.getvalue().splitlines()]
    (density,) = [ln for ln in lines if ln["event"] == "feed.density"]
    assert density["bars_per_symbol"] == {BTC: 3}
    assert density["hour"] == T0.isoformat()


def test_events_are_typed() -> None:
    assert BarClosed(symbol=BTC, timeframe="1m", bar_ts=T0, close=1.0).payload()["symbol"] == BTC
    assert SymbolStale(symbol=BTC, age_s=61.0).payload() == {"symbol": BTC, "age_s": 61.0}
    assert SymbolFresh(symbol=BTC).payload() == {"symbol": BTC}
    assert FeedConnected().payload() == {}
    assert FeedDisconnected().payload() == {}


# --- regressions from the PR #10 re-review ---------------------------------------


async def test_rest_corrections_replace_pending_stream_values(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, announced: list[BarClosed]
) -> None:
    # The verification only merged minutes the stream lacked; a REST correction for a
    # minute the stream already delivered was ignored by the aggregate and the DB.
    clock = Clock(T0 + M)
    source = FakeSource([bar(0, 200)])  # REST says close 200
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0, 100))  # stream said close 100

    clock.now = T0 + 5 * M + GRACE
    await feed.tick()
    await bus.drain()

    (five,) = feed.history(BTC, "5m")
    assert five.close == 200
    async with sessions() as s:
        (stored,) = await repo.get_bars(s, BTC, "1m", since=T0)
    assert stored.close == 200
    assert [e.timeframe for e in announced].count("1m") == 1  # no second 1m announcement


async def test_recovered_bar_refreshes_price_age(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # A bar recovered by verification was persisted and announced, but the symbol stayed
    # stale because only stream input updated the price age.
    clock = Clock(T0 + M)
    source = BrokenLatest([bar(0), bar(4, 104)])  # latest() fails; history() works
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))
    await feed.tick()

    clock.now = T0 + 2 * M + 2 * S
    await feed.tick()
    assert feed.is_stale(BTC)

    clock.now = T0 + 5 * M + GRACE  # minute-4 bar ended 20 s ago (< 60 s)
    await feed.tick()
    await bus.drain()

    assert not feed.is_stale(BTC)
    assert "SymbolFresh" in await event_types(sessions)


async def test_subscribers_see_fresh_state_when_recovered_bars_arrive(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # PR #10 third review: _stale was cleared only after the recovered bars had been
    # announced, so a subscriber (the scanner) saw is_stale() == True for a fresh bar.
    eth = "ETH/USD"
    clock = Clock(T0 + M)
    source = BrokenLatest([bar(0), bar(4, 104), bar(0, symbol=eth), bar(4, 14, symbol=eth)])
    feed = make_feed(source, sessions, bus, clock, symbols=(BTC, eth))
    seen: list[tuple[str, str, bool]] = []

    async def on_closed(e: BarClosed) -> None:
        seen.append((e.symbol, e.timeframe, feed.is_stale(e.symbol)))

    bus.subscribe(BarClosed, on_closed)
    await feed.on_bar(bar(0))
    await feed.on_bar(bar(0, symbol=eth))
    await feed.tick()
    clock.now = T0 + 2 * M + 2 * S
    await feed.tick()
    assert feed.is_stale(BTC) and feed.is_stale(eth)
    await bus.drain()
    seen.clear()

    clock.now = T0 + 5 * M + GRACE
    await feed.tick()
    await bus.drain()

    assert {(s, tf) for s, tf, _ in seen} >= {(BTC, "5m"), (eth, "5m"), (BTC, "1m"), (eth, "1m")}
    assert all(not stale for _, _, stale in seen), seen
    types = await event_types(sessions)
    last_fresh = max(i for i, t in enumerate(types) if t == "SymbolFresh")
    first_recovered = min(
        i for i, t in enumerate(types) if t == "BarClosed" and i > types.index("SymbolStale")
    )
    assert last_fresh < first_recovered  # freshness is announced before the recovered bars


async def test_subscribers_see_fresh_state_for_a_live_bar_ending_a_stale_spell(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # Same ordering bug on the stream path: the 1m bar was announced before the symbol
    # was marked fresh.
    clock = Clock(T0 + M)
    feed = make_feed(BrokenLatest(), sessions, bus, clock)
    seen: list[bool] = []

    async def on_closed(e: BarClosed) -> None:
        seen.append(feed.is_stale(e.symbol))

    await feed.on_bar(bar(0))
    await feed.tick()
    clock.now = T0 + 2 * M + 2 * S
    await feed.tick()
    assert feed.is_stale(BTC)
    bus.subscribe(BarClosed, on_closed)

    clock.now = T0 + 3 * M + 2 * S
    await feed.on_bar(bar(2))
    await bus.drain()

    assert seen == [False]


async def test_quote_and_last_price_include_pending_minutes(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + 30 * M + GRACE)
    feed = make_feed(FakeSource([bar(i, 100 + i) for i in range(30)]), sessions, bus, clock)
    await feed.backfill()
    assert feed.last_price(BTC) == 129
    assert feed.quote(BTC) is None
    await feed.on_bar(bar(30, 150))  # live: pending, not in history yet
    q = Quote(symbol=BTC, ts=clock.now, bid=149.9, ask=150.1)
    await feed.on_quote(q)
    assert feed.last_price(BTC) == 150
    assert feed.quote(BTC) == q
    assert feed.last_price("ETH/USD") is None


# --- bar validation (WP 1.R1a-bars) --------------------------------------------------
# A NaN close became a NaN last_price and reached indicators and the Scanner; a bad bar
# was also persisted, resampled and refreshed price age. bar(m) has high 101, low 99.

INVALID_BARS = pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"close": NAN}, "non-finite"),
        ({"high": INF}, "non-finite"),
        ({"low": -INF}, "non-finite"),
        ({"volume": NAN}, "non-finite"),
        ({"open": 0.0}, "non-positive"),
        ({"low": -1.0}, "non-positive"),
        ({"volume": -1.0}, "negative volume"),
        ({"high": 98.0}, "high < low"),
    ],
    ids=[
        "nan-close",
        "inf-high",
        "neg-inf-low",
        "nan-volume",
        "zero-open",
        "negative-low",
        "negative-volume",
        "inverted-range",
    ],
)


def bad(minute: int, change: dict[str, float], price: float = 100.0) -> Bar:
    return bar(minute, price).model_copy(update=change)


def rejections(logs: list[MutableMapping[str, Any]]) -> list[tuple[object, object]]:
    return [(ln["source"], ln["reason"]) for ln in logs if ln["event"] == "feed.bar_rejected"]


@INVALID_BARS
async def test_invalid_stream_bar_is_dropped_and_does_not_refresh(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    announced: list[BarClosed],
    change: dict[str, float],
    reason: str,
) -> None:
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource(), sessions, bus, clock)
    await feed.on_bar(bar(0))

    with capture_logs() as logs:
        await feed.on_bar(bad(1, change))
    await bus.drain()

    assert rejections(logs) == [("stream", reason)]
    assert await bar_count(sessions) == 1
    assert [e.bar_ts for e in announced] == [T0]
    assert feed.last_price(BTC) == 100
    clock.now = T0 + M + 61 * S  # 61 s after the last *valid* bar ended
    await feed.tick()
    assert feed.is_stale(BTC)


async def test_invalid_late_bar_is_not_persisted(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource([bar(0)]), sessions, bus, clock)
    await feed.on_bar(bar(0))
    clock.now = T0 + 5 * M + GRACE
    await feed.tick()

    with capture_logs() as logs:
        await feed.on_bar(bad(3, {"close": NAN}))

    assert rejections(logs) == [("stream", "non-finite")]
    assert feed.late_bars == 0
    assert await bar_count(sessions) == 1


async def test_invalid_rest_bar_does_not_replace_the_stream_bar(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # REST is authoritative only for bars that pass validation.
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource([bad(0, {"close": NAN})]), sessions, bus, clock)
    await feed.on_bar(bar(0, 100))

    clock.now = T0 + 5 * M + GRACE
    with capture_logs() as logs:
        await feed.tick()

    assert rejections(logs) == [("rest", "non-finite")]
    assert "feed.bar_corrected" not in [ln["event"] for ln in logs]
    (five,) = feed.history(BTC, "5m")
    assert five.close == 100
    async with sessions() as s:
        (stored,) = await repo.get_bars(s, BTC, "1m", since=T0)
    assert stored.close == 100


async def test_invalid_rest_bar_is_not_recovered(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, announced: list[BarClosed]
) -> None:
    # No stream bar for minute 2: the bad REST bar leaves it missing, like a sparse minute.
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource([bar(0), bad(2, {"high": 98.0})]), sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + 5 * M + GRACE
    with capture_logs() as logs:
        await feed.tick()
    await bus.drain()

    assert rejections(logs) == [("rest", "high < low")]
    assert await bar_count(sessions) == 1
    assert [e.bar_ts for e in announced if e.timeframe == "1m"] == [T0]
    (five,) = feed.history(BTC, "5m")
    assert (five.high, five.low, five.volume) == (101, 99, 1.0)


async def test_backfill_drops_invalid_fetched_bars(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + 30 * M + GRACE)
    fetched = [bar(i, 100 + i) for i in range(29)] + [bad(29, {"close": NAN}, 129)]
    feed = make_feed(FakeSource(fetched), sessions, bus, clock)

    with capture_logs() as logs:
        await feed.backfill()

    assert rejections(logs) == [("rest", "non-finite")]
    assert await bar_count(sessions) == 29
    assert len(feed.history(BTC, "1m")) == 29
    assert feed.last_price(BTC) == 128


async def test_backfill_ignores_invalid_rows_already_in_the_db(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # A row persisted before validation existed, for a minute REST no longer serves.
    async with sessions() as s:
        await repo.upsert_bars(s, [bad(29, {"close": -1.0}, 129)])
    clock = Clock(T0 + 30 * M + GRACE)
    feed = make_feed(FakeSource([bar(i, 100 + i) for i in range(29)]), sessions, bus, clock)

    with capture_logs() as logs:
        await feed.backfill()

    assert rejections(logs) == [("db", "non-positive")]
    assert len(feed.history(BTC, "1m")) == 29
    assert feed.last_price(BTC) == 128
    await feed.tick()  # minute 28 ended 80 s ago; the bad row must not count as price age
    assert feed.is_stale(BTC)


async def test_invalid_latest_rest_bar_leaves_the_symbol_stale(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + M + 90 * S
    source.latest_bar[BTC] = bad(1, {"close": NAN})  # would end 30 s ago
    with capture_logs() as logs:
        await feed.tick()

    assert rejections(logs) == [("rest", "non-finite")]
    assert feed.is_stale(BTC)


async def test_valid_rest_quote_is_kept_when_the_latest_rest_bar_is_invalid(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # PR #19 review: bar and quote from one REST refresh are validated independently.
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + M + 90 * S
    source.latest_bar[BTC] = bad(1, {"close": NAN})
    quote = Quote(symbol=BTC, ts=clock.now - 5 * S, bid=99.0, ask=99.1)
    source.latest_quote[BTC] = quote
    with capture_logs() as logs:
        await feed.tick()

    assert rejections(logs) == [("rest", "non-finite")]
    assert feed.quote(BTC) == quote
    assert not feed.is_stale(BTC)  # fresh from the quote, not from the bad bar


async def test_valid_latest_rest_bar_refreshes_price_age(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + M + 90 * S
    source.latest_bar[BTC] = bar(1)  # ended 30 s ago
    await feed.tick()

    assert not feed.is_stale(BTC)


async def test_zero_volume_bar_is_accepted(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # Sparse crypto minutes really do report volume 0.
    feed = make_feed(FakeSource(), sessions, bus, Clock(T0 + M))

    with capture_logs() as logs:
        await feed.on_bar(bad(0, {"volume": 0.0}))

    assert rejections(logs) == []
    assert await bar_count(sessions) == 1
    assert feed.last_price(BTC) == 100


async def test_price_age_is_seconds_since_the_newest_valid_quote_or_bar(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource(), sessions, bus, clock)
    assert feed.price_age_s(BTC) is None

    await feed.on_quote(Quote(symbol=BTC, ts=T0 + M, bid=90.0, ask=90.1))
    assert feed.price_age_s(BTC) == 0.0
    clock.now = T0 + M + 5 * S
    assert feed.price_age_s(BTC) == 5.0

    await feed.on_quote(Quote(symbol=BTC, ts=T0 + M + 4 * S, bid=95.0, ask=94.0))  # crossed
    assert feed.price_age_s(BTC) == 5.0

    await feed.on_bar(bar(2, 100))  # a newer bar end moves the observation forward
    assert feed.price_age_s(BTC) == (clock.now - (bar(2, 100).ts + M)).total_seconds()
