"""Cycle Runner (WP 1.4; spec 2026-10-11-cycle-runner-design.md)."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
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

from autotrader.agents.stub import StubBuy, StubSkip
from autotrader.core.bus import CommitUncertain, EventBus
from autotrader.core.config import StrategyProfile, parse_profile
from autotrader.core.cycle import CycleRunner, CycleSnapshot, CycleState, Decision, _Cursor
from autotrader.core.kill import ControlGate, ControlStatus, write_kill_file
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


# --- admission (Task 6) --------------------------------------------------------------


async def all_cycles(sessions: async_sessionmaker[AsyncSession]) -> list[m.Cycle]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Cycle).order_by(m.Cycle.id)))


def dropped(logs: Sequence[Mapping[str, Any]]) -> list[str]:
    return [e["reason"] for e in logs if e["event"] == "cycle.dropped"]


class OpenGate:
    """Gate double without I/O: race tests need admission to reach its lock in a few
    event-loop turns, which a DB-backed gate read (aiosqlite thread) doesn't guarantee."""

    async def status(self, now: datetime | None = None) -> ControlStatus:
        return ControlStatus(killed=False, reasons=())


def hold_admission(monkeypatch: pytest.MonkeyPatch) -> tuple[asyncio.Event, asyncio.Event]:
    """The first admission blocks inside its transaction until `release` is set."""
    entered, release = asyncio.Event(), asyncio.Event()
    real = repo.open_cycle_symbols
    first = True

    async def held(session: AsyncSession) -> list[str]:
        nonlocal first
        if first:
            first = False
            entered.set()
            await release.wait()
        return await real(session)

    monkeypatch.setattr(repo, "open_cycle_symbols", held)
    return entered, release


async def test_admitted_candidate_gets_a_row_an_event_and_a_task(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, bus, gate, FakeRisk(approved("new")), strategy_id)
    await runner.on_candidate(candidate())
    await runner.join()

    (row,) = await all_cycles(sessions)
    assert (row.symbol, row.rule_id, row.state) == (BTC, "r1", "SUBMITTED")
    assert row.signal_features["_timeframe"] == "5m"
    assert row.signal_features["_bar_ts"] == (NOW - FIVE).isoformat()
    first = (await transitions(sessions, row.id))[0]
    assert (first["from_state"], first["to_state"]) == (None, "NEW")


async def test_concurrency_cap_holds_for_simultaneous_candidates(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    strategy_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = hold_admission(monkeypatch)
    runner = make_runner(
        sessions, bus, OpenGate(), FakeRisk(approved("new")), strategy_id, max_cycles=1
    )
    with capture_logs() as logs:
        first = asyncio.create_task(runner.on_candidate(candidate(BTC)))
        await entered.wait()
        second = asyncio.create_task(runner.on_candidate(candidate(ETH)))
        for _ in range(20):
            await asyncio.sleep(0)
        assert not second.done()  # parked on the admission lock, not already dropped
        release.set()
        await asyncio.gather(first, second)
        await runner.join()
    assert [c.symbol for c in await all_cycles(sessions)] == [BTC]
    assert dropped(logs) == ["max_concurrent_cycles"]


async def test_one_open_cycle_per_symbol(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, bus, gate, FakeRisk(approved("new")), strategy_id)
    with capture_logs() as logs:
        await runner.on_candidate(candidate(BTC))
        await runner.on_candidate(candidate(BTC))
        await runner.join()
    assert len(await all_cycles(sessions)) == 1
    assert dropped(logs) == ["open cycle on symbol"]


async def test_open_entry_order_of_an_error_cycle_blocks_its_symbol(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    cid = await new_cycle(sessions, strategy_id, state="ERROR")
    async with sessions() as s:
        await repo.insert_order(
            s,
            cycle_id=cid,
            client_order_id=f"at-c{cid}-e",
            symbol=BTC,
            mode="paper",
            type_="limit",
            side="buy",
            qty=D("1"),
            limit_price=D("100.1"),
            stop_price=None,
            tp_price=None,
            status="pending",
            now=NOW,
        )
    runner = make_runner(sessions, bus, gate, FakeRisk(), strategy_id)
    with capture_logs() as logs:
        await runner.on_candidate(candidate(BTC))
    assert dropped(logs) == ["open entry order on symbol"]
    assert len(await all_cycles(sessions)) == 1


async def test_stale_candidate_is_dropped(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, bus, gate, FakeRisk(), strategy_id)
    with capture_logs() as logs:
        await runner.on_candidate(candidate(bar_ts=NOW - 3 * FIVE))
    assert dropped(logs) == ["signal stale"]
    assert await all_cycles(sessions) == []


class BrokenGate:
    def __init__(self, hang: bool) -> None:
        self.hang = hang

    async def status(self, now: datetime | None = None) -> ControlStatus:
        if self.hang:
            await asyncio.Event().wait()
        raise RuntimeError("control state unreadable")


@pytest.mark.parametrize(
    ("gate_kind", "reason_prefix"),
    [
        ("killed", "entries blocked"),
        ("raises", "gate unavailable: RuntimeError"),
        ("hangs", "gate unavailable: TimeoutError"),
    ],
)
async def test_gate_blocks_admission_fail_closed(
    gate_kind: str,
    reason_prefix: str,
    tmp_path: Path,
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    use: Any = gate
    if gate_kind == "killed":
        write_kill_file(tmp_path / "KILL", source="cli", reason="test", now=NOW)
    else:
        use = BrokenGate(hang=gate_kind == "hangs")
    runner = make_runner(sessions, bus, use, FakeRisk(), strategy_id, step_timeout_s=0.01)
    with capture_logs() as logs:
        await runner.on_candidate(candidate())
    (reason,) = dropped(logs)
    assert reason.startswith(reason_prefix)
    assert await all_cycles(sessions) == []


async def test_db_error_at_admission_is_logged_not_raised(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def broken(*a: Any, **kw: Any) -> m.Cycle:
        raise OperationalError("INSERT", None, Exception("database is locked"))

    monkeypatch.setattr(repo, "insert_cycle", broken)
    runner = make_runner(sessions, bus, gate, FakeRisk(), strategy_id)
    with capture_logs() as logs:
        await runner.on_candidate(candidate())  # must not raise
    assert any(e["event"] == "cycle.admission_failed" for e in logs)
    assert await all_cycles(sessions) == []


async def test_uncertain_admission_suspends_and_a_late_landing_insert_never_breaks_the_cap(
    sessions: async_sessionmaker[AsyncSession],
    hbus: EventBus,
    hook: CommitHook,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(approved("filled"))
    risk.release = asyncio.Event()
    runner = make_runner(sessions, hbus, gate, risk, strategy_id, max_cycles=2)

    await runner.on_candidate(candidate(SOL))  # a running cycle, held inside check()
    await risk.started.wait()

    hook.fail_before = 1  # BTC's insert does not commit; the read-back sees nothing
    with capture_logs() as logs:
        await runner.on_candidate(candidate(BTC))
        assert runner.admission_suspended
        await runner.on_candidate(candidate(ETH))  # would be admitted without suspension
    assert dropped(logs) == ["admission suspended: commit uncertain"]

    await new_cycle(sessions, strategy_id, symbol=BTC)  # the uncertain insert "lands"
    open_ = [c for c in await all_cycles(sessions) if c.state not in repo.TERMINAL_CYCLE_STATES]
    assert len(open_) <= 2

    risk.release.set()  # cycles already running are unaffected
    await runner.join()
    sol = next(c for c in await all_cycles(sessions) if c.symbol == SOL)
    assert sol.state == "FILLED"


async def test_suspension_is_checked_inside_the_admission_lock(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    strategy_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = hold_admission(monkeypatch)
    runner = make_runner(sessions, bus, OpenGate(), FakeRisk(approved("new")), strategy_id)
    real_publish = runner._bus.publish_tx
    publish_calls = 0

    async def counted(*a: Any, **kw: Any) -> Any:
        nonlocal publish_calls
        if "_admit" in a[0].__qualname__:  # admission units only, not cycle transitions
            publish_calls += 1
        return await real_publish(*a, **kw)

    monkeypatch.setattr(runner._bus, "publish_tx", counted)
    with capture_logs() as logs:
        first = asyncio.create_task(runner.on_candidate(candidate(BTC)))
        await entered.wait()
        second = asyncio.create_task(runner.on_candidate(candidate(ETH)))
        for _ in range(20):
            await asyncio.sleep(0)  # `second` passed the pre-lock checks and waits
        assert runner._admission_lock.locked() and not second.done()
        runner._suspend(CommitUncertain("elsewhere"))
        release.set()
        await asyncio.gather(first, second)
        await runner.join()
    assert [c.symbol for c in await all_cycles(sessions)] == [BTC]
    assert dropped(logs) == ["admission suspended: commit uncertain"]
    assert publish_calls == 1  # `second` was stopped by the in-lock check, never reached the bus


async def test_suspension_by_a_surviving_unit_stops_an_admission_queued_on_the_bus(
    sessions: async_sessionmaker[AsyncSession],
    hbus: EventBus,
    hook: CommitHook,
    strategy_id: int,
) -> None:
    """Review P1 (2026-10-11): an admission that passed the admission-lock check and
    waits for the bus lock must still see a suspension set by the unit ahead of it."""
    runner = make_runner(sessions, hbus, OpenGate(), FakeRisk(approved("new")), strategy_id)
    sol = await new_cycle(sessions, strategy_id, symbol=SOL)
    release = asyncio.Event()
    hook.block_before = release  # SOL's NEW -> ENRICHING unit parks holding the bus lock
    hook.fail_before = 1  # ... and then does not commit; the read-back sees NEW
    running = asyncio.create_task(runner._run(sol, candidate(SOL)))
    await hook.reached.wait()
    running.cancel()  # the publishing caller goes away; the bus-owned unit survives
    with pytest.raises(asyncio.CancelledError):
        await running

    admitting = asyncio.create_task(runner.on_candidate(candidate(BTC)))
    for _ in range(20):
        await asyncio.sleep(0)  # BTC passed both flag checks; its unit waits for the bus
    assert not runner.admission_suspended and not admitting.done()

    with capture_logs() as logs:
        release.set()
        await admitting
    assert runner.admission_suspended
    assert dropped(logs) == ["admission suspended: commit uncertain"]
    assert [c.symbol for c in await all_cycles(sessions)] == [SOL]


# --- recovery (Task 6) ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("state", "with_order", "after", "reason"),
    [
        ("NEW", False, "ERROR", "interrupted"),
        ("ENRICHING", False, "ERROR", "interrupted"),
        ("ANALYZING", False, "ERROR", "interrupted"),
        ("DECIDING", False, "ERROR", "interrupted"),
        ("RISK_CHECK", False, "ERROR", "interrupted"),
        ("RISK_CHECK", True, "RISK_CHECK", None),
        ("SUBMITTED", True, "SUBMITTED", None),
        ("FILLED", True, "FILLED", None),
    ],
)
async def test_recover(
    state: str,
    with_order: bool,
    after: str,
    reason: str | None,
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    cid = await new_cycle(sessions, strategy_id, state=state)
    if with_order:
        async with sessions() as s:
            await repo.insert_order(
                s,
                cycle_id=cid,
                client_order_id=f"at-c{cid}-e",
                symbol=BTC,
                mode="paper",
                type_="limit",
                side="buy",
                qty=D("1"),
                limit_price=D("100.1"),
                stop_price=None,
                tp_price=None,
                status="pending",
                now=NOW,
            )
    await make_runner(sessions, bus, gate, FakeRisk(), strategy_id).recover()
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == (after, reason)


# --- shutdown (Task 7) ---------------------------------------------------------------


async def test_close_cancels_a_cycle_before_check_and_recover_ends_it(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    deciding = asyncio.Event()

    async def hang(c: CandidateSignal, snap: CycleSnapshot) -> Decision:
        deciding.set()
        await asyncio.Event().wait()
        return StubSkip("unreachable")

    runner = make_runner(sessions, bus, gate, FakeRisk(), strategy_id, decide=hang)
    await runner.on_candidate(candidate())
    await deciding.wait()
    report = await runner.close(budget_s=5)
    assert report.abandoned == ()
    (row,) = await all_cycles(sessions)
    assert row.state == "DECIDING"

    await make_runner(sessions, bus, gate, FakeRisk(), strategy_id).recover()
    assert (await cycle_row(sessions, row.id)).state == "ERROR"


async def test_close_drains_a_check_that_finishes_in_time(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(approved("new"))
    risk.release = asyncio.Event()
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    await runner.on_candidate(candidate())
    await risk.started.wait()
    closing = asyncio.create_task(runner.close(budget_s=5, cancel_wait_s=1))
    for _ in range(20):
        await asyncio.sleep(0)
    assert not closing.done()  # draining the check
    risk.release.set()
    report = await closing
    assert report.abandoned == ()
    (row,) = await all_cycles(sessions)
    assert row.state == "SUBMITTED"


async def test_close_forces_a_check_past_the_drain_deadline(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(approved("new"))
    risk.release = asyncio.Event()  # never set: check() only ends by cancellation
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    await runner.on_candidate(candidate())
    await risk.started.wait()
    with capture_logs() as logs:
        report = await runner.close(budget_s=0.05, cancel_wait_s=0.04)
    assert report.abandoned == ()
    assert any(e["event"] == "cycle.forced_cancel" for e in logs)
    (row,) = await all_cycles(sessions)
    assert row.state == "RISK_CHECK"
    # FakeRisk writes no order row, so this test proves the forced cancel only; that a
    # forced cycle keeps a durable, recoverable order is proven on the real engine
    # (Task 8, test_forced_shutdown_after_broker_acceptance_keeps_the_order_recoverable).


async def test_close_reports_a_task_that_ignores_cancellation(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(approved("new"))
    risk.release = asyncio.Event()
    risk.swallow_cancel = True
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    await runner.on_candidate(candidate())
    await risk.started.wait()
    with capture_logs() as logs:
        report = await runner.close(budget_s=0.05, cancel_wait_s=0.04)
    (cid,) = report.abandoned
    assert any(e["event"] == "cycle.task_abandoned" and e["cycle_id"] == cid for e in logs)
    risk.release.set()  # clean up the abandoned check
    await risk.finished.wait()


async def test_close_does_not_wait_for_an_admission_and_that_cycle_never_starts(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = hold_admission(monkeypatch)
    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    admitting = asyncio.create_task(runner.on_candidate(candidate(BTC)))
    await entered.wait()
    report = await asyncio.wait_for(runner.close(budget_s=5), timeout=5)  # safety net only
    assert report.abandoned == ()
    with capture_logs() as logs:
        release.set()
        await admitting
        await runner.on_candidate(candidate(ETH))
    assert any(e["event"] == "cycle.not_started" for e in logs)
    assert dropped(logs) == ["shutting down"]
    (row,) = await all_cycles(sessions)
    assert row.state == "NEW" and risk.calls == []
    await make_runner(sessions, bus, gate, FakeRisk(), strategy_id).recover()
    assert (await cycle_row(sessions, row.id)).state == "ERROR"


async def test_a_step_that_swallows_cancellation_never_starts_check_after_close(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    deciding, swallowed, proceed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def stubborn(c: CandidateSignal, snap: CycleSnapshot) -> Decision:
        deciding.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:  # e.g. an SDK call that eats the cancellation
            swallowed.set()
        await proceed.wait()
        return StubBuy(D("98"), D("104"), D("0.6"), D("1"))

    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id, decide=stubborn)
    await runner.on_candidate(candidate())
    await deciding.wait()
    closing = asyncio.create_task(runner.close(budget_s=5))
    await swallowed.wait()
    proceed.set()  # decide returns after close() began
    report = await closing
    assert report.abandoned == ()
    assert risk.calls == []
    (row,) = await all_cycles(sessions)
    assert row.state == "DECIDING"  # RISK_CHECK was never written

    await make_runner(sessions, bus, gate, FakeRisk(), strategy_id).recover()
    assert (await cycle_row(sessions, row.id)).state == "ERROR"


async def test_risk_check_entered_after_close_started_never_calls_check(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    cid = await new_cycle(sessions, strategy_id, state="RISK_CHECK")
    intent = EntryIntent(
        cycle_id=cid,
        symbol=BTC,
        stop=D("98"),
        take_profit=D("104"),
        confidence=D("0.6"),
        size_fraction=D("1"),
    )
    await runner.close(budget_s=1)
    await runner._risk_check(_Cursor(cid, CycleState.RISK_CHECK), intent)
    assert risk.calls == []
    assert (await cycle_row(sessions, cid)).state == "RISK_CHECK"
    await make_runner(sessions, bus, gate, FakeRisk(), strategy_id).recover()
    assert (await cycle_row(sessions, cid)).state == "ERROR"


async def test_recover_must_run_before_admission(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk()
    risk.release = asyncio.Event()  # keeps the admitted cycle's task alive
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    await runner.on_candidate(candidate())
    await risk.started.wait()
    with pytest.raises(RuntimeError, match="before admission"):
        await runner.recover()
    risk.release.set()
    await runner.join()
