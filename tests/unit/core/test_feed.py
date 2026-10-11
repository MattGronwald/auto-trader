import asyncio
import io
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
)
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
        self.latest_bar_end: dict[str, datetime] = {}
        self.latest_quote: dict[str, datetime] = {}
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
        return Latest(
            bar_end=self.latest_bar_end.get(symbol), quote_ts=self.latest_quote.get(symbol)
        )

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
    source.latest_quote[BTC] = clock.now - 5 * S
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
