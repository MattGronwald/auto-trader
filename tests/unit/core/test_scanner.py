"""WP 1.2 Signal Scanner (SPEC §4.2, PLAN D13/G14)."""

import json
import math
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.bus import EventBus
from autotrader.core.config import StrategyProfile, parse_profile
from autotrader.core.feed import BarClosed
from autotrader.core.kill import ControlGate, write_kill_file
from autotrader.core.scanner import CandidateSignal, Scanner
from autotrader.core.types import Bar
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

PROFILE_PATH = Path(__file__).parents[3] / "strategies" / "fast_momentum_v1.yaml"
BTC, ETH, SOL = "BTC/USD", "ETH/USD", "SOL/USD"
T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
FIVE = timedelta(minutes=5)
N_BARS = 30  # enough for vol_z (20), not for ema50


# --- fixtures ------------------------------------------------------------------------


def make_profile(
    rules: Sequence[dict[str, str]] | None = None, **scanner: float
) -> StrategyProfile:
    data: dict[str, Any] = yaml.safe_load(PROFILE_PATH.read_text())
    data["signal_rules"] = [
        {"direction": "long", **r}
        for r in (rules or [{"id": "up", "expr": "close > 100", "score": "0.5"}])
    ]
    data["scanner"] = {**data["scanner"], **scanner}
    return parse_profile(data)


def series(
    symbol: str = BTC,
    n: int = N_BARS,
    last_close: float = 110.0,
    last_volume: float = 10.0,
    end: datetime = T0 + N_BARS * FIVE,
) -> list[Bar]:
    """5m bars with varying volume; the last one spikes (vol_z well above 0.5)."""
    bars = []
    for i in range(n):
        last = i == n - 1
        close = last_close if last else 100.0
        bars.append(
            Bar(
                symbol=symbol,
                timeframe="5m",
                ts=end - (n - i) * FIVE,
                open=close,
                high=close,
                low=close,
                close=close,
                volume=last_volume if last else 1.0 + i % 2,
            )
        )
    return bars


class FakeMarket:
    def __init__(self) -> None:
        self.bars: dict[tuple[str, str], list[Bar]] = {}
        self.spreads: dict[str, float | None] = {}
        self.stale: set[str] = set()

    def add(self, bars: list[Bar], spread: float | None = 5.0) -> BarClosed:
        self.bars[(bars[0].symbol, bars[0].timeframe)] = bars
        self.spreads[bars[0].symbol] = spread
        last = bars[-1]
        return BarClosed(
            symbol=last.symbol, timeframe=last.timeframe, bar_ts=last.ts, close=last.close
        )

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return list(self.bars.get((symbol, timeframe), []))

    def spread_bps(self, symbol: str) -> float | None:
        return self.spreads.get(symbol)

    def is_stale(self, symbol: str) -> bool:
        return symbol in self.stale


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)
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
async def emitted(bus: EventBus) -> list[CandidateSignal]:
    out: list[CandidateSignal] = []

    async def on(e: CandidateSignal) -> None:
        out.append(e)

    bus.subscribe(CandidateSignal, on)
    return out


@pytest.fixture
def kill_file(tmp_path: Path) -> Path:
    return tmp_path / "control" / "KILL"


@pytest.fixture
def gate(kill_file: Path, sessions: async_sessionmaker[AsyncSession]) -> ControlGate:
    return ControlGate(kill_file, sessions)


@pytest.fixture
def market() -> FakeMarket:
    return FakeMarket()


@pytest.fixture
def clock() -> Clock:
    return Clock(T0 + N_BARS * FIVE)


Factory = Any  # (profile=None) -> Scanner


@pytest.fixture
def scanner(
    market: FakeMarket,
    gate: ControlGate,
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    clock: Clock,
) -> Factory:
    def make(profile: StrategyProfile | None = None) -> Scanner:
        return Scanner(profile or make_profile(), market, gate, sessions, bus, clock=clock)

    return make


async def scan(s: Scanner, bus: EventBus, event: BarClosed) -> None:
    await s.on_bar_closed(event)
    await bus.drain()


async def add_cycle(
    sessions: async_sessionmaker[AsyncSession], symbol: str, state: str = "ANALYZING"
) -> None:
    async with sessions() as session:
        strategy = await repo.register_strategy(session, make_profile(), "yaml")
        session.add(
            m.Cycle(
                strategy_id=strategy.id,
                symbol=symbol,
                rule_id="up",
                signal_features={},
                state=state,
                created_at=T0,
            )
        )
        await session.commit()


async def add_position(
    sessions: async_sessionmaker[AsyncSession], symbol: str, *, closed: bool = False
) -> None:
    async with sessions() as session:
        session.add(
            m.Position(
                symbol=symbol,
                side="long",
                qty=Decimal("0.001"),
                entry_price=Decimal("100"),
                opened_at=T0,
                closed_at=T0 + FIVE if closed else None,
            )
        )
        await session.commit()


# --- emission ------------------------------------------------------------------------


async def test_firing_rule_emits_a_persisted_candidate(
    scanner: Factory,
    market: FakeMarket,
    bus: EventBus,
    emitted: list[CandidateSignal],
    sessions: async_sessionmaker[AsyncSession],
    clock: Clock,
) -> None:
    await scan(scanner(), bus, market.add(series()))

    (c,) = emitted
    assert (c.symbol, c.direction, c.rule_id, c.score) == (BTC, "long", "up", 0.5)
    assert (c.timeframe, c.bar_ts, c.ts) == ("5m", T0 + (N_BARS - 1) * FIVE, clock.now)
    assert c.features["close"] == 110.0
    assert c.features["vol_z"] is not None and c.features["vol_z"] > 0.5
    assert c.features["ema50"] is None  # warm-up NaN -> null, never NaN in JSON
    async with sessions() as session:
        (row,) = await session.scalars(select(m.Event).where(m.Event.type == "CandidateSignal"))
    json.dumps(row.payload, allow_nan=False)  # strict JSON: Postgres rejects NaN


async def test_other_timeframes_are_ignored(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    bars = [b.model_copy(update={"timeframe": "15m"}) for b in series()]
    await scan(scanner(), bus, market.add(bars))
    assert emitted == []


async def test_no_rule_firing_emits_nothing(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    await scan(scanner(), bus, market.add(series(last_close=99.0)))
    assert emitted == []


async def test_best_score_wins_and_profile_order_breaks_ties(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    profile = make_profile(
        [
            {"id": "low", "expr": "close > 100", "score": "0.2"},
            {"id": "first_best", "expr": "close > 100", "score": "0.9"},
            {"id": "second_best", "expr": "close > 100", "score": "0.9"},
        ]
    )
    await scan(scanner(profile), bus, market.add(series()))
    assert [c.rule_id for c in emitted] == ["first_best"]


async def test_rule_reading_a_warmup_nan_is_not_evaluable_not_a_hit(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    profile = make_profile(
        [
            {"id": "needs_ema50", "expr": "close > ema50", "score": "1"},
            {"id": "score_nan", "expr": "close > 100", "score": "ema50 / 100"},
            {"id": "ok", "expr": "close > 100", "score": "0.1"},
        ]
    )
    await scan(scanner(profile), bus, market.add(series()))  # 30 bars < 50
    assert [c.rule_id for c in emitted] == ["ok"]


async def test_evaluates_the_announced_bar_not_a_newer_one(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    bars = series()
    event = market.add(bars)
    newer = bars[-1].model_copy(update={"ts": bars[-1].ts + FIVE, "close": 50.0})
    market.bars[(BTC, "5m")] = [*bars, newer]  # appended before the handler ran
    await scan(scanner(), bus, event)
    assert [c.bar_ts for c in emitted] == [event.bar_ts]


async def test_missing_history_for_the_bar_emits_nothing(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    event = market.add(series())
    market.bars[(BTC, "5m")] = series()[:-1]  # announced bar not in history
    await scan(scanner(), bus, event)
    assert emitted == []


# --- pre-filters ---------------------------------------------------------------------


async def test_bar_older_than_one_signal_period_is_obsolete(
    scanner: Factory,
    market: FakeMarket,
    bus: EventBus,
    emitted: list[CandidateSignal],
    clock: Clock,
) -> None:
    # PR #12 review: reconnect catch-up announces old bars while the symbol reads fresh.
    event = market.add(series())
    bar_end = event.bar_ts + FIVE
    clock.now = bar_end + FIVE + timedelta(seconds=1)
    await scan(scanner(), bus, event)
    assert emitted == []
    clock.now = bar_end + FIVE  # boundary: still actionable
    await scan(scanner(), bus, event)
    assert len(emitted) == 1


async def test_kill_switch_suppresses(
    scanner: Factory,
    market: FakeMarket,
    bus: EventBus,
    emitted: list[CandidateSignal],
    kill_file: Path,
) -> None:
    write_kill_file(kill_file, source="cli", reason="test", now=T0)
    await scan(scanner(), bus, market.add(series()))
    assert emitted == []


async def test_pause_suppresses(
    scanner: Factory,
    market: FakeMarket,
    bus: EventBus,
    emitted: list[CandidateSignal],
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with sessions() as session:
        await repo.set_control(session, "paused_until", (T0 + timedelta(days=1)).isoformat())
    await scan(scanner(), bus, market.add(series()))
    assert emitted == []


async def test_stale_symbol_suppresses(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    event = market.add(series())
    market.stale.add(BTC)
    await scan(scanner(), bus, event)
    assert emitted == []


@pytest.mark.parametrize("spread", [15.01, None])
async def test_wide_or_unknown_spread_suppresses(
    spread: float | None,
    scanner: Factory,
    market: FakeMarket,
    bus: EventBus,
    emitted: list[CandidateSignal],
) -> None:
    await scan(scanner(), bus, market.add(series(), spread=spread))
    assert emitted == []


async def test_spread_at_the_limit_passes(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    await scan(scanner(), bus, market.add(series(), spread=15.0))
    assert len(emitted) == 1


async def test_low_or_undefined_vol_z_suppresses(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    s = scanner()
    await scan(s, bus, market.add(series(last_volume=1.5)))  # ~0 z
    await scan(s, bus, market.add(series(n=10)))  # < 20 bars: vol_z NaN
    assert emitted == []


async def test_open_position_on_symbol_suppresses_closed_one_does_not(
    scanner: Factory,
    market: FakeMarket,
    bus: EventBus,
    emitted: list[CandidateSignal],
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await add_position(sessions, ETH, closed=False)
    await add_position(sessions, BTC, closed=True)
    s = scanner()
    await scan(s, bus, market.add(series(ETH)))
    await scan(s, bus, market.add(series(BTC)))
    assert [c.symbol for c in emitted] == [BTC]


async def test_open_cycle_on_symbol_suppresses_finished_one_does_not(
    scanner: Factory,
    market: FakeMarket,
    bus: EventBus,
    emitted: list[CandidateSignal],
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await add_cycle(sessions, ETH, state="DECIDING")
    for state in ("FILLED", "REJECTED", "EXPIRED", "ERROR"):
        await add_cycle(sessions, BTC, state=state)
    s = scanner()
    await scan(s, bus, market.add(series(ETH)))
    await scan(s, bus, market.add(series(BTC)))
    assert [c.symbol for c in emitted] == [BTC]


async def test_concurrent_cycle_cap_suppresses_other_symbols(
    scanner: Factory,
    market: FakeMarket,
    bus: EventBus,
    emitted: list[CandidateSignal],
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await add_cycle(sessions, ETH)
    await add_cycle(sessions, SOL)  # 2 open = max_concurrent_cycles
    await scan(scanner(), bus, market.add(series(BTC)))
    assert emitted == []


async def test_rate_limit_is_a_rolling_hour_and_survives_restart(
    scanner: Factory,
    market: FakeMarket,
    bus: EventBus,
    emitted: list[CandidateSignal],
    clock: Clock,
) -> None:
    async def scan_now() -> None:  # a fresh bar that has just closed, as the feed delivers
        await scan(scanner(), bus, market.add(series(end=clock.now)))  # new instance each time

    await scan_now()
    clock.now += timedelta(minutes=10)
    await scan_now()  # counts come from the journal, not instance state
    clock.now += timedelta(minutes=10)
    await scan_now()  # third within the hour: suppressed
    assert len(emitted) == 2
    clock.now = emitted[0].ts + timedelta(hours=1)  # first one leaves the window
    await scan_now()
    assert len(emitted) == 3


async def test_rate_limit_is_per_symbol(
    scanner: Factory, market: FakeMarket, bus: EventBus, emitted: list[CandidateSignal]
) -> None:
    profile = make_profile(max_candidates_per_symbol_per_hour=1)
    s = scanner(profile)
    await scan(s, bus, market.add(series(BTC)))
    await scan(s, bus, market.add(series(BTC)))
    await scan(s, bus, market.add(series(ETH)))
    assert [c.symbol for c in emitted] == [BTC, ETH]


def test_candidate_features_reject_nan() -> None:
    with pytest.raises(ValueError):
        CandidateSignal(
            symbol=BTC,
            direction="long",
            rule_id="x",
            score=1.0,
            timeframe="5m",
            bar_ts=T0,
            features={"close": math.nan},
        )
