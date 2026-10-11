"""Cycle Runner on a real RiskEngine + SimBroker (WP 1.4; spec §10).

SimBroker acks every accepted order `new` (fills happen on a later bar), so an approved
cycle ends in SUBMITTED here; completing it is WP 1.5's reconciliation.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.broker.base import AssetRules, BrokerOrder, FeeSchedule, OrderAck
from autotrader.broker.sim import SimBroker
from autotrader.core.bus import EventBus
from autotrader.core.config import StrategyProfile, parse_profile
from autotrader.core.cycle import CycleRunner
from autotrader.core.kill import ControlGate
from autotrader.core.scanner import CandidateSignal
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import RiskEngine

PROFILE_PATH = Path(__file__).resolve().parents[2] / "strategies" / "fast_momentum_v1.yaml"
BTC = "BTC/USD"
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
FIVE = timedelta(minutes=5)
RULES = AssetRules(
    min_qty=D("0.0001"),
    qty_increment=D("0.0001"),
    min_notional=D("1"),
    price_increment=D("0.01"),
)
FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"))


def profile() -> StrategyProfile:
    data: dict[str, Any] = yaml.safe_load(PROFILE_PATH.read_text())
    return parse_profile(data)


def five(i: int) -> Bar:  # high - low = 2 -> ATR 2
    return Bar(
        symbol=BTC,
        timeframe="5m",
        ts=NOW - (30 - i) * FIVE,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1.0,
    )


class View:
    """Feed stand-in for both the runner and the engine."""

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return [five(i) for i in range(30)] if (symbol, timeframe) == (BTC, "5m") else []

    def quote(self, symbol: str) -> Quote | None:
        return Quote(symbol=BTC, ts=NOW, bid=99.98, ask=100.0) if symbol == BTC else None

    def last_price(self, symbol: str) -> float | None:
        return 100.0 if symbol == BTC else None

    def is_stale(self, symbol: str) -> bool:
        return False

    def spread_bps(self, symbol: str) -> float | None:
        return 2.0 if symbol == BTC else None


def candidate() -> CandidateSignal:
    return CandidateSignal(
        ts=NOW,
        symbol=BTC,
        direction="long",
        rule_id="breakout",
        score=0.8,
        timeframe="5m",
        bar_ts=NOW - FIVE,
        features={"close": 100.0},
    )


class AcceptThenHang(SimBroker):
    """The broker accepts the order, but the acknowledgement never comes back."""

    def __init__(self) -> None:
        super().__init__(
            cash=D("1000"), fees=FEES, slippage_bps=D("0"), asset_rules={BTC: RULES}, timeframe="5m"
        )
        self.accepted = asyncio.Event()
        self.submits = 0

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        self.submits += 1
        await super().submit_order(order)  # now it exists at the broker
        self.accepted.set()
        await asyncio.Event().wait()  # ...but the ack never reaches the engine
        raise AssertionError("unreachable")


def sim_broker() -> SimBroker:
    sim = SimBroker(
        cash=D("1000"), fees=FEES, slippage_bps=D("0"), asset_rules={BTC: RULES}, timeframe="5m"
    )
    return sim


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    # Sync fixture: Alembic's upgrade runs its own event loop (asyncio.run), which fails
    # inside an async fixture's running loop.
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)
    return url


@pytest.fixture
async def sessions(db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = make_engine(db_url)
    yield make_sessionmaker(engine)
    await engine.dispose()


async def wire(
    tmp_path: Path, sessions: async_sessionmaker[AsyncSession], broker: SimBroker
) -> tuple[CycleRunner, EventBus]:
    prof = profile()
    async with sessions() as s:
        strategy_id = (await repo.register_strategy(s, prof, "yaml")).id
    broker.process_bar(five(29))
    gate = ControlGate(tmp_path / "KILL", sessions)
    view = View()
    engine = RiskEngine(
        broker,
        profile=prof,
        strategy_id=strategy_id,
        mode="paper",
        gate=gate,
        market=view,
        sessions=sessions,
        clock=lambda: NOW,
    )
    bus = EventBus(sessions)
    runner = CycleRunner(
        profile=prof,
        strategy_id=strategy_id,
        market=view,
        gate=gate,
        risk=engine,
        bus=bus,
        sessions=sessions,
        clock=lambda: NOW,
    )
    return runner, bus


@pytest.fixture
async def wired(
    tmp_path: Path, sessions: async_sessionmaker[AsyncSession]
) -> AsyncIterator[tuple[CycleRunner, SimBroker]]:
    sim = sim_broker()
    runner, bus = await wire(tmp_path, sessions, sim)
    yield runner, sim
    await runner.close(budget_s=5)
    await bus.close()


async def test_candidate_reaches_the_broker_and_waits_in_submitted(
    wired: tuple[CycleRunner, SimBroker], sessions: async_sessionmaker[AsyncSession]
) -> None:
    runner, _ = wired
    await runner.on_candidate(candidate())
    await runner.join()
    async with sessions() as s:
        (cycle,) = list(await s.scalars(select(m.Cycle)))
        (order,) = list(await s.scalars(select(m.Order)))
        events = list(
            await s.scalars(
                select(m.Event).where(m.Event.cycle_id == cycle.id).order_by(m.Event.id)
            )
        )
    assert cycle.state == "SUBMITTED"
    assert (order.cycle_id, order.status, order.client_order_id) == (
        cycle.id,
        "new",
        f"at-c{cycle.id}-e",
    )
    assert [e.payload["to_state"] for e in events if e.type == "CycleTransitioned"] == [
        "NEW",
        "ENRICHING",
        "ANALYZING",
        "DECIDING",
        "RISK_CHECK",
        "SUBMITTED",
    ]


async def test_broker_accepts_then_the_ack_write_fails(
    wired: tuple[CycleRunner, SimBroker],
    sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, sim = wired

    async def lost(*a: Any, **kw: Any) -> None:
        raise OperationalError("UPDATE orders", None, Exception("disk I/O error"))

    monkeypatch.setattr(repo, "update_order", lost)  # after submit_order() succeeded
    await runner.on_candidate(candidate())
    await runner.join()

    async with sessions() as s:
        (cycle,) = list(await s.scalars(select(m.Cycle)))
        (order,) = list(await s.scalars(select(m.Order)))
    assert (cycle.state, cycle.reject_reason) == ("ERROR", "RISK_CHECK: OperationalError")
    assert (order.status, order.client_order_id) == ("pending", f"at-c{cycle.id}-e")
    assert (await sim.get_order("sim-1")).status == "new"  # it really is at the broker

    with capture_logs() as logs:
        await runner.on_candidate(candidate())
    assert [e["reason"] for e in logs if e["event"] == "cycle.dropped"] == [
        "open entry order on symbol"
    ]


async def test_forced_shutdown_after_broker_acceptance_keeps_the_order_recoverable(
    tmp_path: Path, sessions: async_sessionmaker[AsyncSession]
) -> None:
    """Spec §8: forcing check() is safe because the order row is durable before submit."""
    broker = AcceptThenHang()
    runner, bus = await wire(tmp_path, sessions, broker)  # engine io budget: 10 s default
    await runner.on_candidate(candidate())
    await broker.accepted.wait()  # at the broker; the ack write has not happened

    with capture_logs() as logs:
        report = await runner.close(budget_s=0.1, cancel_wait_s=0.05)
    await bus.close()
    assert report.abandoned == ()
    assert any(e["event"] == "cycle.forced_cancel" for e in logs)

    async with sessions() as s:
        (cycle,) = list(await s.scalars(select(m.Cycle)))
        (order,) = list(await s.scalars(select(m.Order)))
    assert cycle.state == "RISK_CHECK"
    assert (order.status, order.client_order_id, order.broker_order_id) == (
        "pending",
        f"at-c{cycle.id}-e",
        None,
    )
    assert broker.submits == 1
    assert (await broker.get_order("sim-1")).status == "new"  # really at the broker

    # Next start: recovery must not end it, and the symbol stays reserved.
    runner2, bus2 = await wire(tmp_path, sessions, broker)
    await runner2.recover()
    async with sessions() as s:
        assert await repo.cycle_state(s, cycle.id) == "RISK_CHECK"
    with capture_logs() as logs:
        await runner2.on_candidate(candidate())
    assert [e["reason"] for e in logs if e["event"] == "cycle.dropped"] == ["open cycle on symbol"]
    assert broker.submits == 1  # no second submission
    await runner2.close(budget_s=5)
    await bus2.close()
