"""Scanner on the real MarketFeed (PR #12 review): FakeMarket hid the feed's ordering."""

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.bus import EventBus
from autotrader.core.config import parse_profile
from autotrader.core.feed import BarClosed, Latest, MarketFeed
from autotrader.core.kill import ControlGate
from autotrader.core.scanner import CandidateSignal, Scanner
from autotrader.core.types import Bar, Quote
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

REPO = Path(__file__).resolve().parents[2]
BTC = "BTC/USD"
T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
M = timedelta(minutes=1)
GRACE = timedelta(seconds=20)


def minute(i: int, close: float) -> Bar:
    v = 1.0 + i % 2  # 5m sums alternate 7/8: vol_z is defined
    return Bar(
        symbol=BTC,
        timeframe="1m",
        ts=T0 + i * M,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=v,
    )


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class RestSource:
    def __init__(self, bars: Sequence[Bar]) -> None:
        self.bars = list(bars)
        self.is_connected = True

    async def history(self, symbols: Sequence[str], start: datetime, end: datetime) -> list[Bar]:
        return [b for b in self.bars if b.symbol in symbols and start <= b.ts < end]

    async def latest(self, symbol: str) -> Latest:
        return Latest(bar_end=None, quote=None)

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


async def test_reconnect_backlog_yields_only_the_current_bar(
    sessions: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    data: dict[str, Any] = yaml.safe_load((REPO / "strategies/fast_momentum_v1.yaml").read_text())
    data["timeframes"] = {"signal": "5m", "context": [], "trend": "5m"}
    data["signal_rules"] = [{"id": "up", "expr": "close > 100", "direction": "long", "score": "1"}]
    data["scanner"] = {**data["scanner"], "min_vol_z": -1000}  # isolate the age rule
    profile = parse_profile(data)

    warm = [minute(i, 100.0) for i in range(200)]  # rule false
    gap = [minute(i, 110.0) for i in range(200, 320)]  # rule true, 2 h of 5m bars
    source = RestSource(warm)
    clock = Clock(T0 + 200 * M + GRACE)
    bus = EventBus(sessions)
    feed = MarketFeed(
        source,
        sessions,
        bus,
        symbols=[BTC],
        timeframes=["5m"],
        stale_alert_s=60,
        lookback=10,
        clock=clock,
    )
    scanner = Scanner(
        profile, feed, ControlGate(tmp_path / "KILL", sessions), sessions, bus, clock=clock
    )
    emitted: list[CandidateSignal] = []
    announced: list[BarClosed] = []

    async def on_candidate(e: CandidateSignal) -> None:
        emitted.append(e)

    async def on_closed(e: BarClosed) -> None:
        if e.timeframe == "5m":
            announced.append(e)

    bus.subscribe(CandidateSignal, on_candidate)
    bus.subscribe(BarClosed, on_closed)
    bus.subscribe(BarClosed, scanner.on_bar_closed)
    try:
        await feed.backfill()
        await feed.tick()  # connected
        source.is_connected = False
        await feed.tick()  # outage: nothing is finalised while blind

        source.bars += gap
        clock.now = T0 + 320 * M + GRACE
        source.is_connected = True
        await feed.on_quote(Quote(symbol=BTC, ts=clock.now, bid=109.99, ask=110.01))
        await feed.tick()  # verifies the whole gap and announces its 24 5m bars
        await bus.drain()
    finally:
        await bus.close()

    assert len(announced) == 24
    assert [c.bar_ts for c in emitted] == [T0 + 315 * M]  # only the bar that just closed
