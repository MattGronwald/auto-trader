"""Cycle Runner (WP 1.4; spec 2026-10-11-cycle-runner-design.md)."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.agents.stub import StubBuy, StubSkip
from autotrader.core.bus import EventBus
from autotrader.core.config import StrategyProfile, parse_profile
from autotrader.core.cycle import CycleRunner, CycleSnapshot, Decision
from autotrader.core.kill import ControlGate
from autotrader.core.scanner import CandidateSignal
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import OrderResult
from autotrader.risk.types import EntryIntent, RiskDecision
from tests.dbhooks import CommitHook, hooked_sessionmaker

PROFILE_PATH = Path(__file__).resolve().parents[3] / "strategies" / "fast_momentum_v1.yaml"
BTC, ETH, SOL = "BTC/USD", "ETH/USD", "SOL/USD"
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
FIVE = timedelta(minutes=5)


def profile(max_cycles: int = 2) -> StrategyProfile:
    data: dict[str, Any] = yaml.safe_load(PROFILE_PATH.read_text())
    data["scanner"] = {**data["scanner"], "max_concurrent_cycles": max_cycles}
    return parse_profile(data)


def bars(symbol: str, spread: float = 1.0) -> list[Bar]:
    """30 flat 5m bars ending at NOW; high - low = 2 x spread, so ATR14 = 2 x spread."""
    return [
        Bar(
            symbol=symbol,
            timeframe="5m",
            ts=NOW - (30 - i) * FIVE,
            open=100.0,
            high=100.0 + spread,
            low=100.0 - spread,
            close=100.0,
            volume=1.0,
        )
        for i in range(30)
    ]


@dataclass
class Market:
    stale: set[str] = field(default_factory=set)
    quotes: dict[str, Quote | None] = field(
        default_factory=lambda: {
            s: Quote(symbol=s, ts=NOW, bid=99.98, ask=100.0) for s in (BTC, ETH, SOL)
        }
    )
    series: dict[str, list[Bar]] = field(
        default_factory=lambda: {s: bars(s) for s in (BTC, ETH, SOL)}
    )

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return list(self.series.get(symbol, [])) if timeframe == "5m" else []

    def quote(self, symbol: str) -> Quote | None:
        return self.quotes.get(symbol)

    def last_price(self, symbol: str) -> float | None:
        q = self.quotes.get(symbol)
        return None if q is None else q.ask

    def is_stale(self, symbol: str) -> bool:
        return symbol in self.stale

    def spread_bps(self, symbol: str) -> float | None:  # for the real RiskEngine (Task 8)
        q = self.quotes.get(symbol)
        return None if q is None else (q.ask - q.bid) / q.ask * 10_000


class Clock:
    def __init__(self, now: datetime = NOW) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def approved(status: str | None) -> OrderResult:
    return OrderResult(
        RiskDecision(approved=True, reasons=(), qty=D("1")),
        order_row_id=1,
        status=status,
        broker_order_id="b-1",
    )


class FakeRisk:
    """`EntryChecker` double: records calls, optionally blocks until `release` is set."""

    def __init__(self, result: OrderResult | Exception | None = None) -> None:
        self.result: OrderResult | Exception = result if result is not None else approved("new")
        self.calls: list[EntryIntent] = []
        self.release: asyncio.Event | None = None
        self.swallow_cancel = False
        self.started = asyncio.Event()
        self.finished = asyncio.Event()

    async def check(self, intent: EntryIntent) -> OrderResult:
        self.calls.append(intent)
        self.started.set()
        try:
            while self.release is not None:
                try:
                    await self.release.wait()
                    break
                except asyncio.CancelledError:
                    if not self.swallow_cancel:
                        raise
            if isinstance(self.result, Exception):
                raise self.result
            return self.result
        finally:
            self.finished.set()


def candidate(
    symbol: str = BTC, score: float = 0.8, bar_ts: datetime = NOW - FIVE
) -> CandidateSignal:
    return CandidateSignal(
        ts=NOW,
        symbol=symbol,
        direction="long",
        rule_id="r1",
        score=score,
        timeframe="5m",
        bar_ts=bar_ts,
        features={"close": 100.0, "vol_z": None},
    )


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
def hook() -> CommitHook:
    return CommitHook()


@pytest.fixture
async def hbus(db_url: str, hook: CommitHook) -> AsyncIterator[EventBus]:
    engine = make_engine(db_url)
    b = EventBus(hooked_sessionmaker(engine, hook))
    yield b
    await b.close()
    await engine.dispose()


@pytest.fixture
def gate(tmp_path: Path, sessions: async_sessionmaker[AsyncSession]) -> ControlGate:
    return ControlGate(tmp_path / "KILL", sessions)


@pytest.fixture
async def strategy_id(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as s:
        return (await repo.register_strategy(s, profile(), "yaml")).id


def make_runner(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: Any,
    risk: Any,
    strategy_id: int,
    *,
    max_cycles: int = 2,
    market: Market | None = None,
    clock: Clock | None = None,
    decide: Callable[[CandidateSignal, CycleSnapshot], Awaitable[Decision]] | None = None,
    step_timeout_s: float = 30.0,
) -> CycleRunner:
    return CycleRunner(
        profile=profile(max_cycles),
        strategy_id=strategy_id,
        market=market or Market(),
        gate=gate,
        risk=risk,
        bus=bus,
        sessions=sessions,
        decide=decide,
        clock=clock or Clock(),
        step_timeout_s=step_timeout_s,
    )


async def new_cycle(
    sessions: async_sessionmaker[AsyncSession],
    strategy_id: int,
    symbol: str = BTC,
    state: str = "NEW",
) -> int:
    async with sessions() as s:
        row = await repo.insert_cycle(
            s,
            strategy_id=strategy_id,
            symbol=symbol,
            rule_id="r1",
            signal_features={},
            state=state,
            now=NOW,
        )
        await s.commit()
        return row.id


async def cycle_row(sessions: async_sessionmaker[AsyncSession], cycle_id: int) -> m.Cycle:
    async with sessions() as s:
        row = await s.get(m.Cycle, cycle_id, populate_existing=True)
        assert row is not None
        return row


async def transitions(
    sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> list[dict[str, Any]]:
    async with sessions() as s:
        rows = await s.scalars(
            select(m.Event)
            .where(m.Event.type == "CycleTransitioned", m.Event.cycle_id == cycle_id)
            .order_by(m.Event.id)
        )
        return [r.payload for r in rows]


def edges(payloads: list[dict[str, Any]]) -> list[tuple[str | None, str]]:
    return [(p["from_state"], p["to_state"]) for p in payloads]


# --- FSM (Task 5) --------------------------------------------------------------------

PATH_TO_SUBMITTED = [
    ("NEW", "ENRICHING"),
    ("ENRICHING", "ANALYZING"),
    ("ANALYZING", "DECIDING"),
    ("DECIDING", "RISK_CHECK"),
    ("RISK_CHECK", "SUBMITTED"),
]


async def test_approved_entry_ends_submitted_with_a_full_audit_trail(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(approved("new"))
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    cid = await new_cycle(sessions, strategy_id)

    await runner._run(cid, candidate())

    assert (await cycle_row(sessions, cid)).state == "SUBMITTED"
    trail = await transitions(sessions, cid)
    assert edges(trail) == PATH_TO_SUBMITTED
    assert trail[2]["details"] == {"skipped": "no analyst in phase 1"}
    intent_details = trail[3]["details"]
    assert D(intent_details["stop"]) == D("97.1")
    assert D(intent_details["take_profit"]) == D("106.1")
    assert trail[4]["details"]["status"] == "new"
    (intent,) = risk.calls
    assert (intent.cycle_id, intent.symbol, intent.confidence) == (cid, BTC, D("0.50"))


@pytest.mark.parametrize(
    ("status", "final", "reason"),
    [
        ("filled", "FILLED", None),
        ("rejected", "REJECTED", "order rejected"),
        ("not_submitted", "REJECTED", "order not_submitted"),
        ("new", "SUBMITTED", None),
        ("unknown", "SUBMITTED", None),
        ("canceled", "SUBMITTED", None),  # an IOC cancel may follow a partial fill
    ],
)
async def test_order_status_mapping(
    status: str,
    final: str,
    reason: str | None,
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, bus, gate, FakeRisk(approved(status)), strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == (final, reason)


async def test_risk_rejection_ends_rejected_with_the_first_reason(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    rejected = OrderResult(RiskDecision(approved=False, reasons=("expectancy -1 <= 0", "x")))
    runner = make_runner(sessions, bus, gate, FakeRisk(rejected), strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("REJECTED", "expectancy -1 <= 0")


async def test_check_exception_is_error_and_never_retried(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(RuntimeError("boom"))
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("ERROR", "RISK_CHECK: RuntimeError")
    assert len(risk.calls) == 1


async def test_stub_skip_is_rejected(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate(score=0.3))
    row = await cycle_row(sessions, cid)
    assert row.state == "REJECTED"
    assert row.reject_reason is not None and row.reject_reason.startswith("decision: skip (")
    assert risk.calls == []


@pytest.mark.parametrize(
    ("tweak", "reason"),
    [
        ("stale", "symbol stale"),
        ("no_quote", "no ask"),
        ("zero_ask", "no ask"),
        ("nan_ask", "no ask"),
        ("no_bars", "ATR missing"),
        ("flat_bars", "ATR not positive"),
    ],
)
async def test_enrichment_failures_expire(
    tweak: str,
    reason: str,
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    market = Market()
    if tweak == "stale":
        market.stale.add(BTC)
    elif tweak == "no_quote":
        market.quotes[BTC] = None
    elif tweak == "zero_ask":
        market.quotes[BTC] = Quote(symbol=BTC, ts=NOW, bid=0.0, ask=0.0)
    elif tweak == "nan_ask":
        market.quotes[BTC] = Quote(symbol=BTC, ts=NOW, bid=99.0, ask=float("nan"))
    elif tweak == "no_bars":
        market.series[BTC] = []
    else:
        market.series[BTC] = bars(BTC, spread=0.0)
    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id, market=market)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("EXPIRED", reason)
    assert risk.calls == []


async def test_signal_gone_stale_while_deciding_expires_without_check(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    clock = Clock()

    async def slow_decide(c: CandidateSignal, snap: CycleSnapshot) -> Decision:
        clock.now = NOW + timedelta(minutes=6)  # > one period after the bar ended
        return StubBuy(
            stop=D("97"), take_profit=D("106"), confidence=D("0.5"), size_fraction=D("1")
        )

    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id, clock=clock, decide=slow_decide)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("EXPIRED", "signal stale")
    assert edges(await transitions(sessions, cid))[-1] == ("RISK_CHECK", "EXPIRED")
    assert risk.calls == []


async def test_step_timeout_is_error_and_not_retried(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    calls = 0

    async def hang(c: CandidateSignal, snap: CycleSnapshot) -> Decision:
        nonlocal calls
        calls += 1
        await asyncio.Event().wait()
        return StubSkip("unreachable")

    runner = make_runner(
        sessions, bus, gate, FakeRisk(), strategy_id, decide=hang, step_timeout_s=0.01
    )
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("ERROR", "DECIDING: timeout")
    assert calls == 1


def fail_transition_to(monkeypatch: pytest.MonkeyPatch, to_state: str) -> None:
    """The first write of a transition into `to_state` fails; later writes are real."""
    real = repo.transition_cycle
    failed = False

    async def flaky(session: AsyncSession, cycle_id: int, **kw: Any) -> None:
        nonlocal failed
        if kw["to_state"] == to_state and not failed:
            failed = True
            raise RuntimeError("disk I/O error")
        await real(session, cycle_id, **kw)

    monkeypatch.setattr(repo, "transition_cycle", flaky)


async def test_write_failure_before_check_falls_back_to_error(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fail_transition_to(monkeypatch, "ANALYZING")
    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("ERROR", "transition write failed")
    assert risk.calls == []


@pytest.mark.parametrize(
    ("status", "failing", "stays"),
    [("new", "SUBMITTED", "RISK_CHECK"), ("filled", "FILLED", "SUBMITTED")],
)
async def test_write_failure_after_check_keeps_the_last_committed_state(
    status: str,
    failing: str,
    stays: str,
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fail_transition_to(monkeypatch, failing)
    runner = make_runner(sessions, bus, gate, FakeRisk(approved(status)), strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    with capture_logs() as logs:
        await runner._run(cid, candidate())
    assert (await cycle_row(sessions, cid)).state == stays
    assert all(p["to_state"] != "ERROR" for p in await transitions(sessions, cid))
    assert any(e["event"] == "cycle.stuck" for e in logs)


async def test_uncertain_commit_on_a_transition_suspends_admission_without_fallback(
    sessions: async_sessionmaker[AsyncSession],
    hbus: EventBus,
    hook: CommitHook,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, hbus, gate, FakeRisk(), strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    hook.fail_before = 1  # NEW -> ENRICHING does not commit; read-back sees NEW
    await runner._run(cid, candidate())
    assert runner.admission_suspended
    assert (await cycle_row(sessions, cid)).state == "NEW"
    assert await transitions(sessions, cid) == []  # no fallback ERROR write
