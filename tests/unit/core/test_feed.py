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
    def __init__(self, bars: Sequence[Bar] = ()) -> None:
        self.bars = list(bars)
        self.history_calls: list[tuple[tuple[str, ...], datetime, datetime]] = []
        self.latest_bar_end: dict[str, datetime] = {}
        self.latest_quote: dict[str, datetime] = {}
        self.latest_calls = 0
        self.is_connected = True

    async def history(self, symbols: Sequence[str], start: datetime, end: datetime) -> list[Bar]:
        self.history_calls.append((tuple(symbols), start, end))
        return [b for b in self.bars if b.symbol in symbols and start <= b.ts < end]

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
    clock = Clock(T0 + 30 * M + timedelta(seconds=20))
    source = FakeSource([bar(i, 100 + i) for i in range(30)])
    feed = make_feed(source, sessions, bus, clock)

    await feed.backfill()
    await bus.drain()

    assert await bar_count(sessions) == 30
    assert [b.close for b in feed.history(BTC, "1m")][-3:] == [127, 128, 129]
    assert [b.ts for b in feed.history(BTC, "5m")] == [T0 + i * 5 * M for i in range(6)]
    assert await event_types(sessions) == []  # old bars must not trigger the scanner


async def test_backfill_only_fetches_what_the_db_lacks(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    async with sessions() as s:
        await repo.upsert_bars(s, [bar(i) for i in range(10)])
    clock = Clock(T0 + 30 * M)
    source = FakeSource([bar(i) for i in range(30)])

    await make_feed(source, sessions, bus, clock).backfill()

    (_, start, end) = source.history_calls[0]
    assert start == T0 + 10 * M
    assert end == T0 + 30 * M  # the current, still forming minute is excluded


# --- live bars -----------------------------------------------------------------------


async def test_live_bar_is_persisted_and_announced_once(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M + timedelta(seconds=3))
    feed = make_feed(FakeSource(), sessions, bus, clock)

    await feed.on_bar(bar(0))
    await feed.on_bar(bar(0))  # duplicate (backfill overlap / reconnect)
    await bus.drain()

    assert await bar_count(sessions) == 1
    assert await event_types(sessions) == ["BarClosed"]


async def test_tick_closes_higher_timeframes_by_time(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    received: list[BarClosed] = []

    async def on_closed(e: BarClosed) -> None:
        received.append(e)

    bus.subscribe(BarClosed, on_closed)
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource(), sessions, bus, clock)
    await feed.on_bar(bar(1, 100))  # minutes 0, 2, 3, 4 have no bar (sparse feed)

    clock.now = T0 + 5 * M + timedelta(seconds=11)
    await feed.tick()
    await bus.drain()

    assert [(e.timeframe, e.bar_ts) for e in received] == [("1m", T0 + M), ("5m", T0)]
    assert feed.history(BTC, "5m")[-1].close == 100


# --- quotes / spread -----------------------------------------------------------------


async def test_quote_updates_spread(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    feed = make_feed(FakeSource(), sessions, bus, Clock(T0))

    await feed.on_quote(Quote(symbol=BTC, ts=T0, bid=99.95, ask=100.05))

    assert feed.spread_bps(BTC) == pytest.approx(10.0)
    assert feed.spread_bps("ETH/USD") is None


# --- staleness -----------------------------------------------------------------------


async def test_symbol_goes_stale_once_and_recovers(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    feed = make_feed(FakeSource(), sessions, bus, clock)
    await feed.on_bar(bar(0))
    assert not feed.is_stale(BTC)

    clock.now = T0 + M + timedelta(seconds=61)
    await feed.tick()
    clock.now += timedelta(seconds=1)
    await feed.tick()
    assert feed.is_stale(BTC)

    await feed.on_bar(bar(2))
    await bus.drain()

    assert not feed.is_stale(BTC)
    types = await event_types(sessions)
    assert types.count("SymbolStale") == 1
    assert types[-1] == "SymbolFresh" or types[-2] == "SymbolFresh"


async def test_never_seen_symbol_is_stale(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    feed = make_feed(FakeSource(), sessions, bus, Clock(T0))

    assert feed.is_stale("ETH/USD")


async def test_rest_refresh_prevents_false_stale_on_a_quiet_market(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # Sparse venue: no stream data for >60 s, but REST shows a recent quote.
    clock = Clock(T0 + M)
    source = FakeSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + M + timedelta(seconds=90)
    source.latest_quote[BTC] = clock.now - timedelta(seconds=5)
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

    clock.now = T0 + M + timedelta(seconds=61)
    for _ in range(30):  # 30 ticks within one stale window
        await feed.tick()
        clock.now += timedelta(seconds=1)

    assert source.latest_calls == 1


async def test_rest_showing_a_missed_bar_triggers_gap_fill(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource([bar(0), bar(3, 103)])
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))

    clock.now = T0 + 4 * M + timedelta(seconds=5)
    source.latest_bar_end[BTC] = T0 + 4 * M  # end of the minute-3 bar the stream never delivered
    await feed.tick()

    assert await bar_count(sessions) == 2
    assert feed.history(BTC, "1m")[-1].close == 103


# --- connection ----------------------------------------------------------------------


async def test_disconnect_holds_resampling_and_reconnect_backfills_the_gap(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = FakeSource([bar(0, 100), bar(2, 102), bar(4, 104)])
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0, 100))
    await feed.tick()  # connected (ticks run every second in production)

    source.is_connected = False
    clock.now = T0 + 6 * M
    await feed.tick()
    assert feed.history(BTC, "5m") == []  # not closed flat while blind

    source.is_connected = True
    await feed.tick()
    await bus.drain()

    assert await bar_count(sessions) == 3  # minutes 2 and 4 fetched via REST
    (five,) = feed.history(BTC, "5m")
    assert (five.high, five.close) == (105, 104)  # real data, not a flat fill
    types = await event_types(sessions)
    last_connected = len(types) - 1 - types[::-1].index("FeedConnected")
    assert types.index("FeedDisconnected") < last_connected
    assert types.count("FeedDisconnected") == 1


async def test_events_are_typed(sessions: async_sessionmaker[AsyncSession], bus: EventBus) -> None:
    assert BarClosed(symbol=BTC, timeframe="1m", bar_ts=T0, close=1.0).payload()["symbol"] == BTC
    assert SymbolStale(symbol=BTC, age_s=61.0).payload() == {"symbol": BTC, "age_s": 61.0}
    assert SymbolFresh(symbol=BTC).payload() == {"symbol": BTC}
    assert FeedConnected().payload() == {}
    assert FeedDisconnected().payload() == {}


async def test_not_yet_connected_at_startup_is_not_an_outage(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    # Live run: the first tick ran before the websocket connected, which logged a
    # spurious FeedDisconnected + reconnect gap fill on every startup.
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
    assert source.history_calls == []  # no gap fill for a connection that never existed


class BrokenSource(FakeSource):
    async def latest(self, symbol: str) -> Latest:
        raise ConnectionError("rest down")

    async def history(self, symbols: Sequence[str], start: datetime, end: datetime) -> list[Bar]:
        raise ConnectionError("rest down")


async def test_rest_failures_are_logged_not_raised(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + M)
    source = BrokenSource()
    feed = make_feed(source, sessions, bus, clock)
    await feed.on_bar(bar(0))
    await feed.tick()

    source.is_connected = False
    clock.now = T0 + 3 * M
    await feed.tick()
    source.is_connected = True
    await feed.tick()  # reconnect gap fill fails -> logged
    clock.now += timedelta(seconds=120)
    await feed.tick()  # REST refresh fails -> logged, symbol goes stale

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


async def test_bar_of_next_bucket_announces_the_closed_one(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    received: list[tuple[str, datetime]] = []

    async def on_closed(e: BarClosed) -> None:
        received.append((e.timeframe, e.bar_ts))

    bus.subscribe(BarClosed, on_closed)
    feed = make_feed(FakeSource(), sessions, bus, Clock(T0 + 6 * M))

    await feed.on_bar(bar(0))
    await feed.on_bar(bar(5))  # first bar of the next 5m bucket closes the previous one
    await bus.drain()

    assert received == [("1m", T0), ("1m", T0 + 5 * M), ("5m", T0)]
