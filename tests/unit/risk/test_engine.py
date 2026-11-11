"""RiskEngine on SimBroker, a migrated SQLite journal and the real ControlGate."""

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any, NoReturn, TypeVar, cast

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

import autotrader.risk.engine as engine_module
from autotrader.broker.base import (
    Account,
    AssetRules,
    Broker,
    BrokerOrder,
    DustRemainder,
    FeeSchedule,
    Fill,
    OrderAck,
    OrderMethodsUnavailable,
    OrderRejected,
    OrderStatus,
    Position,
)
from autotrader.broker.sim import FillPlan, SimBroker
from autotrader.core.bus import EventBus
from autotrader.core.config import StrategyProfile, load_profile
from autotrader.core.kill import (
    PAUSED_KEY,
    PAUSED_UNTIL_KEY,
    ControlGate,
    ControlStatus,
    write_kill_file,
)
from autotrader.core.ledger import Ledger
from autotrader.core.tradingday import day_start, next_day_start
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.checks import check_entry
from autotrader.risk.engine import Accounting, Gate, OrderResult, RiskEngine
from autotrader.risk.types import (
    CancelResult,
    EntryIntent,
    ExitRequest,
    MarketSnapshot,
    OpenEntry,
    PortfolioState,
    RiskDecision,
    SyncResult,
)
from tests.dbhooks import CommitHook, hooked_sessionmaker
from tests.health import seed_baseline

REPO = Path(__file__).resolve().parents[3]
T = TypeVar("T")
PROFILE: StrategyProfile = load_profile(REPO / "strategies" / "fast_momentum_v1.yaml")
BTC = "BTC/USD"
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
RULES = AssetRules(
    min_qty=D("0.0001"), qty_increment=D("0.0001"), min_notional=D("1"), price_increment=D("0.01")
)
FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"), buy_fee_asset="quote")


def five(i: int, close: float = 100.0) -> Bar:
    # range 2 per bar -> ATR 2 (no gaps)
    return Bar(
        symbol=BTC,
        timeframe="5m",
        ts=NOW - (30 - i) * timedelta(minutes=5),
        open=close,
        high=close + 1,
        low=close - 1,
        close=close,
        volume=1.0,
    )


@dataclass
class FakeView:
    bars: list[Bar] = field(default_factory=lambda: [five(i) for i in range(30)])
    last: float | None = 100.0
    q: Quote | None = field(default_factory=lambda: Quote(symbol=BTC, ts=NOW, bid=99.98, ask=100.0))
    stale: bool = False
    prices: dict[str, float] = field(default_factory=dict)  # last prices besides BTC
    stale_symbols: frozenset[str] = frozenset()

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return list(self.bars) if symbol == BTC and timeframe == "5m" else []

    def quote(self, symbol: str) -> Quote | None:
        return self.q if symbol == BTC else None

    def last_price(self, symbol: str) -> float | None:
        return self.last if symbol == BTC else self.prices.get(symbol)

    def is_stale(self, symbol: str) -> bool:
        return self.stale or symbol in self.stale_symbols


class BrokenView(FakeView):
    """Every feed accessor raises: the feed object itself is broken."""

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        raise RuntimeError("feed broken")

    def quote(self, symbol: str) -> Quote | None:
        raise RuntimeError("feed broken")

    def last_price(self, symbol: str) -> float | None:
        raise RuntimeError("feed broken")

    def is_stale(self, symbol: str) -> bool:
        raise RuntimeError("feed broken")


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


@pytest.fixture(autouse=True)
async def _baseline(
    request: pytest.FixtureRequest, sessions: async_sessionmaker[AsyncSession]
) -> None:
    """A trusted day-start baseline (1000, 60 s before NOW's UTC day start) unless the test
    builds its own equity snapshots (`no_baseline`)."""
    if request.node.get_closest_marker("no_baseline") is None:
        await seed_baseline(sessions, NOW)


@pytest.fixture
def sim() -> SimBroker:
    s = SimBroker(
        cash=D("1000"), fees=FEES, slippage_bps=D("0"), asset_rules={BTC: RULES}, timeframe="5m"
    )
    s.process_bar(five(29))
    return s


@pytest.fixture
async def cycle_id(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            created_at=NOW,
        )
        s.add(row)
        await s.commit()
        return row.id


def make_engine_(
    broker: Broker,
    sessions: async_sessionmaker[AsyncSession],
    gate: Gate,
    view: FakeView | None = None,
    clock: Clock | None = None,
    tz: str = "UTC",
    io_timeout_s: float = 10.0,
    monotonic: "Mono | None" = None,
    cls: type[RiskEngine] = RiskEngine,
    ledger: Accounting | None = None,
    healthy: bool = True,
    order_visibility_allowance_s: float = 60.0,
    order_resolution_timeout_s: float = 80.0,
) -> RiskEngine:
    engine = cls(
        broker,
        profile=PROFILE,
        strategy_id=1,
        mode="paper",
        gate=gate,
        market=view or FakeView(),
        sessions=sessions,
        ledger=ledger or Ledger(EventBus(sessions), sessions, clock=clock or Clock(NOW)),
        trading_day_tz=tz,
        clock=clock or Clock(NOW),
        io_timeout_s=io_timeout_s,
        monotonic=time.monotonic if monotonic is None else monotonic,
        order_visibility_allowance_s=order_visibility_allowance_s,
        order_resolution_timeout_s=order_resolution_timeout_s,
    )
    if healthy:  # a fresh loop snapshot and a reconciled cash step (spec 2026-11-09 4, 5.2)
        engine.last_snapshot_at = (time.monotonic if monotonic is None else monotonic)()
        engine.cash_reconciled()
    return engine


def intent(cycle_id: int, **kw: object) -> EntryIntent:
    base = dict(
        cycle_id=cycle_id,
        symbol=BTC,
        stop=D("98"),
        take_profit=D("106"),
        confidence=D("0.6"),
        size_fraction=D("1"),
    )
    return EntryIntent(**{**base, **kw})


async def orders(sessions: async_sessionmaker[AsyncSession]) -> list[m.Order]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Order).order_by(m.Order.id)))


async def decisions(sessions: async_sessionmaker[AsyncSession]) -> list[m.RiskDecision]:
    async with sessions() as s:
        return list(await s.scalars(select(m.RiskDecision).order_by(m.RiskDecision.id)))


@pytest.fixture
def gate(tmp_path: Path, sessions: async_sessionmaker[AsyncSession]) -> ControlGate:
    return ControlGate(tmp_path / "KILL", sessions)


async def _jid(sessions: async_sessionmaker[AsyncSession]) -> str:
    async with sessions() as s:
        jid = await repo.journal_id(s)
    assert jid is not None
    return jid


async def test_entry_client_id_carries_the_journal_id(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    jid = await _jid(sessions)
    assert len(jid) == 8
    first = make_engine_(sim, sessions, gate)
    assert (await first.check(intent(cycle_id))).decision.approved
    (row,) = await orders(sessions)
    assert row.client_order_id == f"at-{jid}-c{cycle_id}-e"
    second = make_engine_(_sim(), sessions, gate)  # a restart on the same journal
    assert await second._journal_id() == jid


async def test_missing_journal_id_rejects_entries_but_not_exits(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    await _held_btc(sim)
    async with sessions() as s:
        await s.execute(delete(m.ControlState).where(m.ControlState.key == repo.JOURNAL_ID_KEY))
        await s.commit()
    engine = make_engine_(sim, sessions, gate)
    result = await engine.check(intent(cycle_id))
    assert result.decision.reasons == ("state unavailable: RuntimeError",)
    assert await orders(sessions) == []
    with capture_logs() as logs:  # reduce-only exits must stay possible
        exited = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))
    assert exited.decision.approved and exited.status == "new"
    (row,) = await orders(sessions)
    assert row.client_order_id is None
    assert any(e["event"] == "risk.journal_id_missing" and e["symbol"] == BTC for e in logs)


class SellSpy(SimBroker):
    """Records the orders submitted and any `close_position` call."""

    submitted: list[BrokerOrder]
    closes: list[str]

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        self.submitted.append(order)
        return await super().submit_order(order)

    async def close_position(self, symbol: str) -> OrderAck:
        self.closes.append(symbol)
        return await super().close_position(symbol)


async def test_exit_submits_a_namespaced_market_sell_of_the_free_qty(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    broker = SellSpy(  # a base-fee buy leaves an off-increment holding (1.0001 less 0.25 %)
        cash=D("1000"),
        fees=FeeSchedule(maker_bps=D(15), taker_bps=D(25), buy_fee_asset="base"),
        slippage_bps=D(0),
        asset_rules={BTC: RULES},
        timeframe="5m",
    )
    broker.process_bar(five(29))
    broker.submitted, broker.closes = [], []
    await broker.submit_order(BrokerOrder(symbol=BTC, side="buy", qty=D("1.0001"), type="market"))
    broker.process_bar(five(30))
    broker.submitted.clear()
    (held,) = await broker.get_positions()
    assert held.qty % RULES.qty_increment != 0  # the precondition this test is about
    result = await make_engine_(broker, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert result.decision.approved and result.status == "new"
    (row,) = await orders(sessions)
    (sent,) = broker.submitted
    assert sent.client_order_id == f"at-{await _jid(sessions)}-o{row.id}"
    assert row.client_order_id == sent.client_order_id
    assert (sent.side, sent.type, sent.qty) == ("sell", "market", held.qty_available)
    assert broker.closes == []
    broker.process_bar(five(31))
    assert await broker.get_positions() == []  # no holding and no dust remain


class GatedSubmit(SimBroker):
    """`submit_order` blocks on `release` for sells; records the engine's in-flight set."""

    entered: asyncio.Event
    release: asyncio.Event

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        self.entered.set()
        await self.release.wait()
        return await super().submit_order(order)


async def test_in_flight_set_covers_insert_to_return(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = cast(GatedSubmit, _sim(GatedSubmit))
    broker.entered, broker.release = asyncio.Event(), asyncio.Event()
    engine = make_engine_(broker, sessions, gate)
    task = asyncio.create_task(engine.check(intent(cycle_id)))
    await asyncio.wait_for(broker.entered.wait(), 10)
    (row,) = await orders(sessions)
    assert engine._in_flight == {row.id}
    broker.release.set()
    result = await task
    assert result.status == "new" and engine._in_flight == set()


async def test_in_flight_set_is_empty_after_a_gate_rejection(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, StuckGate("raise"))
    rejected = await engine.check(intent(cycle_id))
    assert rejected.status == "not_submitted" and engine._in_flight == set()


async def test_in_flight_set_covers_an_exit_too(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    broker = cast(GatedSubmit, _sim(GatedSubmit))
    broker.entered, broker.release = asyncio.Event(), asyncio.Event()
    broker.release.set()
    await _held_btc(broker)
    broker.entered.clear()
    broker.release.clear()
    engine = make_engine_(broker, sessions, gate)
    task = asyncio.create_task(engine.exit(ExitRequest(symbol=BTC, reason="stop")))
    await asyncio.wait_for(broker.entered.wait(), 10)
    (row,) = await orders(sessions)
    assert engine._in_flight == {row.id}
    broker.release.set()
    assert (await task).status == "new" and engine._in_flight == set()


def cancel_at_insert_close(
    sessions: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> async_sessionmaker[AsyncSession]:
    """A session factory whose session that inserted an order row raises `CancelledError`
    as it closes (the row is committed): a cancellation at the session's last await."""
    armed: list[bool] = []
    real_insert = repo.insert_order

    async def insert(session: AsyncSession, **kw: Any) -> m.Order:
        row = await real_insert(session, **kw)
        armed.append(True)
        return row

    monkeypatch.setattr(repo, "insert_order", insert)

    @asynccontextmanager
    async def factory() -> AsyncIterator[AsyncSession]:
        async with sessions() as session:
            yield session
        if armed:
            armed.clear()
            raise asyncio.CancelledError

    return cast(async_sessionmaker[AsyncSession], factory)


async def test_in_flight_set_is_empty_after_a_cancel_at_entry_session_close(
    sim: SimBroker,
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = make_engine_(sim, cancel_at_insert_close(sessions, monkeypatch), gate)
    with pytest.raises(asyncio.CancelledError):
        await engine.check(intent(cycle_id))
    assert len(await orders(sessions)) == 1  # the row was inserted, then the close cancelled
    assert engine._in_flight == set()


async def test_in_flight_set_is_empty_after_a_cancel_at_exit_session_close(
    sim: SimBroker,
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _held_btc(sim)
    engine = make_engine_(sim, cancel_at_insert_close(sessions, monkeypatch), gate)
    with pytest.raises(asyncio.CancelledError):
        await engine.exit(ExitRequest(symbol=BTC, reason="stop"))
    assert len(await orders(sessions)) == 1
    assert engine._in_flight == set()


async def test_approved_entry_is_persisted_and_submitted(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    result = await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert result.decision.approved and result.status == "new"
    (row,) = await orders(sessions)
    assert (row.client_order_id, row.status, row.broker_order_id) == (
        f"at-{await _jid(sessions)}-c{cycle_id}-e",
        "new",
        "sim-1",
    )
    assert (row.side, row.type, row.qty, row.limit_price) == (
        "buy",
        "limit",
        D("2.997"),
        D("100.10"),
    )
    assert (row.stop_price, row.tp_price, row.mode) == (D("98"), D("106"), "paper")
    (dec,) = await decisions(sessions)
    assert (dec.kind, dec.approved, dec.computed_size, dec.cycle_id) == (
        "entry",
        True,
        D("2.997"),
        cycle_id,
    )


@pytest.mark.no_baseline
async def test_first_check_snapshots_equity_as_baseline(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    """The first-ever snapshot is written, but a post-start row is no daily-loss baseline."""
    result = await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert result.decision.reasons == ("daily-loss baseline unavailable",)
    async with sessions() as s:
        snap = await repo.latest_snapshot(s)
    assert snap is not None and (snap.equity, snap.hwm, snap.daily_pnl) == (
        D("1000"),
        D("1000"),
        D(0),
    )


async def test_daily_loss_sets_paused_until_next_day_start_in_trading_tz(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    async with sessions() as s:  # baseline 1040 before the Berlin day start -> loss 40 >= 31.2
        await repo.insert_equity_snapshot(
            s,
            ts=NOW - timedelta(hours=14),
            equity=D("1040"),
            cash=D("1040"),
            exposure=D(0),
            hwm=D("1040"),
            daily_pnl=D(0),
        )
    result = await make_engine_(sim, sessions, gate, tz="Europe/Berlin").check(intent(cycle_id))
    assert not result.decision.approved and "daily loss" in result.decision.reasons[0]
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_UNTIL_KEY) == "2026-10-09T22:00:00+00:00"
    assert await orders(sessions) == []


def _sim(cls: type[SimBroker] = SimBroker) -> SimBroker:
    broker = cls(
        cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m"
    )
    broker.process_bar(five(29))
    return broker


async def new_cycle(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            created_at=NOW,
        )
        s.add(row)
        await s.commit()
        return row.id


class EquityBroker(SimBroker):
    """Reports `equity` instead of the simulated one when it is set (cash unchanged)."""

    equity: D | None = None
    positions_down = False  # get_positions raises (after the account was read)

    async def get_account(self) -> Account:
        account = await super().get_account()
        if self.equity is None:
            return account
        return account.model_copy(update={"equity": self.equity})

    async def get_positions(self) -> list[Position]:
        if self.positions_down:
            raise ConnectionError("positions endpoint down")
        return await super().get_positions()


async def snapshots(sessions: async_sessionmaker[AsyncSession]) -> list[m.EquitySnapshot]:
    async with sessions() as s:
        return list(await s.scalars(select(m.EquitySnapshot).order_by(m.EquitySnapshot.ts)))


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("seen_by", ["invalid_stop", "gather_failure"])
async def test_equity_peak_seen_by_a_rejected_check_still_bounds_drawdown(
    seen_by: str,
    restart: bool,
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
) -> None:
    broker = _sim(EquityBroker)
    assert isinstance(broker, EquityBroker)
    clock = Clock(NOW)
    engine = make_engine_(broker, sessions, gate, clock=clock)
    await engine.snapshot_equity()  # baseline and HWM 1000
    broker.equity = D("1200")  # spike between two snapshots
    clock.now = NOW + timedelta(seconds=10)
    if seen_by == "invalid_stop":
        spiked = await engine.check(intent(cycle_id, stop=D("100.10")))
        assert "not below entry" in spiked.decision.reasons[0]
    else:  # the account read succeeds, the positions read after it fails
        broker.positions_down = True
        spiked = await engine.check(intent(cycle_id))
        assert spiked.decision.reasons == ("state unavailable: ConnectionError",)
        broker.positions_down = False
    rows = await snapshots(sessions)
    assert [s.hwm for s in rows] == [D("1000"), D("1000"), D("1200")]  # seed, loop, peak
    assert rows[2].exposure is None  # an account observation: exposure unknown
    broker.equity = D("1000")  # back down before the next periodic snapshot
    clock.now = NOW + timedelta(seconds=20)
    if restart:
        engine = make_engine_(broker, sessions, gate, clock=clock)
    result = await engine.check(intent(await new_cycle(sessions)))
    assert not result.decision.approved and "drawdown" in result.decision.reasons[0]
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_KEY) is True
    assert await orders(sessions) == []


class ParkingPositions(EquityBroker):
    """The first armed `get_positions` parks until the test releases it; later calls
    (e.g. a concurrent snapshot's) pass straight through."""

    parked: asyncio.Event | None = None
    release: asyncio.Event | None = None

    async def get_positions(self) -> list[Position]:
        if self.parked is not None and self.release is not None and not self.parked.is_set():
            self.parked.set()
            await self.release.wait()
        return await super().get_positions()


async def test_peak_committed_by_a_concurrent_snapshot_bounds_the_parked_check(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    """Codex round 3: the check read HWM 1000 early, then waited on positions while the
    snapshot task committed a 1200 peak; equity fell back before the check resumed."""
    broker = _sim(ParkingPositions)
    assert isinstance(broker, ParkingPositions)
    clock = Clock(NOW)
    engine = make_engine_(broker, sessions, gate, clock=clock, monotonic=Mono())
    await engine.snapshot_equity()  # baseline and HWM 1000
    broker.parked, broker.release = asyncio.Event(), asyncio.Event()
    clock.now = NOW + timedelta(seconds=10)
    entry = asyncio.create_task(engine.check(intent(cycle_id)))  # reads equity/HWM 1000
    try:
        await asyncio.wait_for(broker.parked.wait(), 2)  # parked in get_positions
        broker.equity = D("1200")
        clock.now = NOW + timedelta(seconds=20)
        await engine.snapshot_equity()  # commits the 1200 peak meanwhile
        broker.equity = D("1000")  # and it falls back before the check resumes
        broker.release.set()
        result = await asyncio.wait_for(entry, 2)
    finally:
        entry.cancel()
        await asyncio.gather(entry, return_exceptions=True)
    assert not result.decision.approved and "drawdown" in result.decision.reasons[0]
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_KEY) is True
    assert await orders(sessions) == []


@pytest.mark.no_baseline
async def test_snapshot_records_the_peak_even_when_positions_fail(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    broker = _sim(EquityBroker)
    assert isinstance(broker, EquityBroker)
    broker.equity, broker.positions_down = D("1200"), True
    engine = make_engine_(broker, sessions, gate)
    with pytest.raises(ConnectionError, match="positions endpoint down"):
        await engine.snapshot_equity()
    ((row),) = await snapshots(sessions)
    assert (row.equity, row.hwm, row.exposure) == (D("1200"), D("1200"), None)


async def test_hwm_is_the_maximum_of_all_snapshots_not_the_latest(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    """An interleaved writer that read before the peak was stored wrote a lower HWM later:
    neither the next snapshot nor a check may fall back to it."""
    for minutes, hwm in ((-2, "1200"), (-1, "1000")):
        async with sessions() as s:
            await repo.insert_equity_snapshot(
                s,
                ts=NOW + timedelta(minutes=minutes),
                equity=D("1000"),
                cash=D("1000"),
                exposure=D(0),
                hwm=D(hwm),
                daily_pnl=D(0),
            )
    engine = make_engine_(_sim(), sessions, gate)
    await engine.snapshot_equity()  # equity 1000
    assert (await snapshots(sessions))[-1].hwm == D("1200")
    result = await engine.check(intent(cycle_id))
    assert "drawdown" in result.decision.reasons[0]


async def test_drawdown_sets_manual_pause(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    async with sessions() as s:
        await repo.insert_equity_snapshot(
            s,
            ts=NOW - timedelta(minutes=1),
            equity=D("1000"),
            cash=D("1000"),
            exposure=D(0),
            hwm=D("1200"),
            daily_pnl=D(0),
        )
    result = await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert "drawdown" in result.decision.reasons[0]
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_KEY) is True


class BrokenBroker(SimBroker):
    async def get_account(self):  # type: ignore[no-untyped-def]
        raise ConnectionError("broker down")


async def test_state_failure_rejects_fail_closed(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = BrokenBroker(
        cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m"
    )
    result = await make_engine_(broker, sessions, gate).check(intent(cycle_id))
    assert result.decision.reasons == ("state unavailable: ConnectionError",)
    (dec,) = await decisions(sessions)
    assert not dec.approved


class FlippingGate:
    """Allows the first status() call, then reports a kill."""

    def __init__(self) -> None:
        self.calls = 0

    async def status(self, now: datetime | None = None) -> ControlStatus:
        self.calls += 1
        if self.calls == 1:
            return ControlStatus(killed=False, reasons=())
        return ControlStatus(killed=True, reasons=("kill file present",))


async def test_kill_between_gathering_and_submit_never_reaches_broker(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    result = await make_engine_(sim, sessions, FlippingGate()).check(intent(cycle_id))
    assert result.status == "not_submitted"
    (row,) = await orders(sessions)
    assert row.status == "not_submitted" and row.broker_order_id is None


class RejectingBroker(SimBroker):
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        raise OrderRejected("insufficient balance")


class TimeoutBroker(SimBroker):
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        raise TimeoutError


@pytest.mark.parametrize(
    ("cls", "status"), [(RejectingBroker, "rejected"), (TimeoutBroker, "unknown")]
)
async def test_submit_failures(
    cls: type[SimBroker],
    status: str,
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
) -> None:
    broker = cls(
        cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m"
    )
    broker.process_bar(five(29))
    result = await make_engine_(broker, sessions, gate).check(intent(cycle_id))
    assert result.status == status
    assert (await orders(sessions))[0].status == status


async def test_unknown_order_counts_as_open_and_blocks_the_symbol(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = TimeoutBroker(
        cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m"
    )
    broker.process_bar(five(29))
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            created_at=NOW,
        )
        s.add(row)
        await s.commit()
        second = row.id
    result = await engine.check(intent(second))
    assert "already has a position or open entry" in result.decision.reasons[0]


async def test_acked_entry_is_refreshed_from_broker(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate)
    await engine.check(intent(cycle_id))
    sim.process_bar(
        five(30, close=101.5).model_copy(update={"low": 101.0, "high": 102.0})
    )  # no fill: canceled
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            created_at=NOW,
        )
        s.add(row)
        await s.commit()
        second = row.id
    result = await engine.check(intent(second))
    statuses = [o.status for o in await orders(sessions)]
    assert statuses[0] == "canceled"  # refreshed from the broker
    assert result.decision.approved, result.decision.reasons  # no longer blocks BTC


async def test_acked_entry_still_new_keeps_blocking_its_symbol(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate)
    await engine.check(intent(cycle_id))  # acked "new"; no bar processed since
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            created_at=NOW,
        )
        s.add(row)
        await s.commit()
        second = row.id
    result = await engine.check(intent(second))
    assert not result.decision.approved
    assert "already has a position or open entry" in result.decision.reasons[0]
    assert [o.status for o in await orders(sessions)] == ["new"]


class OrderingBroker(SimBroker):
    """Reports positions only once get_order has been called (a fill seen late)."""

    order_polled = False

    async def get_order(self, order_id: str) -> OrderStatus:
        self.order_polled = True
        return await super().get_order(order_id)

    async def get_positions(self) -> list[Position]:
        return await super().get_positions() if self.order_polled else []


async def test_own_orders_are_refreshed_before_positions_are_read(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = OrderingBroker(
        cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m"
    )
    broker.process_bar(five(29))
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))
    broker.process_bar(five(30, close=100.0))  # the entry fills at the broker
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            created_at=NOW,
        )
        s.add(row)
        await s.commit()
        second = row.id
    result = await engine.check(intent(second))
    assert not result.decision.approved
    assert "already has a position or open entry" in result.decision.reasons[0]


class PartialFillBroker(SimBroker):
    """Reports every order as partially filled (an IOC never rests there in SimBroker)."""

    async def get_order(self, order_id: str) -> OrderStatus:
        status = await super().get_order(order_id)
        return status.model_copy(update={"status": "partially_filled"})


async def test_partially_filled_entry_stays_open_and_is_recorded(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = PartialFillBroker(
        cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m"
    )
    broker.process_bar(five(29))
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            created_at=NOW,
        )
        s.add(row)
        await s.commit()
        second = row.id

    for _ in range(2):  # first poll records the change, the second sees it unchanged
        result = await engine.check(intent(second))
        assert "already has a position or open entry" in result.decision.reasons[0]
        assert [o.status for o in await orders(sessions)] == ["partially_filled"]


async def _held_btc(sim: SimBroker) -> None:
    await sim.submit_order(BrokerOrder(symbol=BTC, side="buy", qty=D("1"), type="market"))
    sim.process_bar(five(30))


async def test_exit_goes_through_while_killed(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, tmp_path: Path
) -> None:
    await _held_btc(sim)
    write_kill_file(tmp_path / "KILL", source="cli", reason="test", now=NOW)
    result = await make_engine_(sim, sessions, gate).exit(ExitRequest(symbol=BTC, reason="kill"))
    assert result.decision.approved and result.status == "new"
    (row,) = await orders(sessions)
    assert (row.side, row.type, row.qty, row.client_order_id) == (
        "sell",
        "market",
        D("1"),
        f"at-{await _jid(sessions)}-o{row.id}",
    )
    (dec,) = await decisions(sessions)
    assert (dec.kind, dec.cycle_id) == ("exit", None)


async def test_exit_without_position_is_rejected_and_recorded(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    result = await make_engine_(sim, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert not result.decision.approved
    assert await orders(sessions) == []
    (dec,) = await decisions(sessions)
    assert not dec.approved


async def test_exit_state_failure_rejects(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    class NoPositions(SimBroker):
        async def get_positions(self) -> list[Position]:
            raise ConnectionError

    broker = NoPositions(
        cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m"
    )
    result = await make_engine_(broker, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert result.decision.reasons == ("state unavailable: ConnectionError",)
    (dec,) = await decisions(sessions)
    assert (dec.kind, dec.approved, dec.cycle_id) == ("exit", False, None)
    assert await orders(sessions) == []


async def test_exit_needs_no_feed_but_entries_fail_closed_without_one(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    # exit() priced the held positions through the feed, so a broken feed accessor made a
    # protective exit fail as "state unavailable".
    await _held_btc(sim)
    engine = make_engine_(sim, sessions, gate, view=BrokenView())

    entry = await engine.check(intent(cycle_id))
    assert entry.decision.reasons == ("state unavailable: RuntimeError",)
    exited = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))

    assert exited.decision.approved and exited.status == "new"
    (row,) = await orders(sessions)
    assert (row.symbol, row.side, row.qty) == (BTC, "sell", D("1"))


async def test_exit_does_not_depend_on_unrelated_positions(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    eth = "ETH/USD"

    class FlakyEthRules(SimBroker):
        async def asset_rules(self, symbol: str) -> AssetRules:
            if symbol == eth:
                raise ConnectionError("rules endpoint down")
            return await super().asset_rules(symbol)

    broker = FlakyEthRules(
        cash=D("1000"),
        fees=FEES,
        slippage_bps=D(0),
        asset_rules={BTC: RULES, eth: RULES},
        timeframe="5m",
    )
    for i in (29, 30):  # bar 29 gives each symbol a price, bar 30 fills the market buys
        for symbol in (BTC, eth):
            broker.process_bar(five(i).model_copy(update={"symbol": symbol}))
            if i == 29:
                order = BrokerOrder(symbol=symbol, side="buy", qty=D("1"), type="market")
                await broker.submit_order(order)
    result = await make_engine_(broker, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert result.decision.approved and result.status == "new"
    (row,) = await orders(sessions)
    assert (row.symbol, row.side) == (BTC, "sell")


@pytest.mark.no_baseline
async def test_snapshot_tracks_hwm_and_daily_pnl(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    clock = Clock(NOW)
    engine = make_engine_(sim, sessions, gate, clock=clock)
    await engine.snapshot_equity()  # baseline 1000
    await _held_btc(sim)  # 1 BTC @ 100 + fee
    sim.process_bar(five(31, close=120.0))  # marked up
    clock.now = NOW + timedelta(minutes=1)
    await engine.snapshot_equity()
    sim.process_bar(five(32, close=90.0))
    clock.now = NOW + timedelta(minutes=2)
    await engine.snapshot_equity()
    async with sessions() as s:
        rows = list(await s.scalars(select(m.EquitySnapshot).order_by(m.EquitySnapshot.ts)))
    assert [r.hwm for r in rows] == [D("1000"), rows[1].equity, rows[1].equity]
    assert rows[2].daily_pnl == rows[2].equity - D("1000")
    assert rows[2].exposure == D("100.0")  # marked at the feed view's last price (100)


# --- time budget: a hung call must not hold the lock forever ---------------------------


async def hang() -> NoReturn:
    await asyncio.Event().wait()  # never set: a stuck REST call
    raise AssertionError("unreachable")


class HangingAccount(SimBroker):
    entered: asyncio.Event | None = None
    release: asyncio.Event | None = None  # set by the test: the call then times out

    async def get_account(self) -> Account:
        if self.entered is not None:
            self.entered.set()
        if self.release is None:
            await hang()
        await self.release.wait()
        raise TimeoutError


async def test_hung_entry_gather_cannot_block_an_exit(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = _sim(HangingAccount)
    assert isinstance(broker, HangingAccount)
    broker.entered, broker.release = asyncio.Event(), asyncio.Event()
    await _held_btc(broker)
    engine = make_engine_(broker, sessions, gate, monotonic=Mono())  # 10 s budget
    entry = asyncio.create_task(engine.check(intent(cycle_id)))
    try:
        await broker.entered.wait()  # the entry holds its lock inside the hung call
        exited = await asyncio.wait_for(engine.exit(ExitRequest(symbol=BTC, reason="stop")), 2)
        assert exited.decision.approved and exited.status == "new"
        assert not entry.done()  # the exit did not wait for the entry at all
        broker.release.set()  # now let the entry's broker call time out
        checked = await asyncio.wait_for(entry, 2)
    finally:
        entry.cancel()
        await asyncio.gather(entry, return_exceptions=True)
    assert checked.decision.reasons == ("state unavailable: TimeoutError",)
    assert sorted((d.kind, d.approved) for d in await decisions(sessions)) == [
        ("entry", False),
        ("exit", True),
    ]
    assert [(o.side, o.status) for o in await orders(sessions)] == [("sell", "new")]


class HangingSubmit(SimBroker):
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        await hang()


async def test_hung_entry_submit_is_unknown(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(_sim(HangingSubmit), sessions, gate, io_timeout_s=1.0, monotonic=Mono())
    result = await asyncio.wait_for(engine.check(intent(cycle_id)), 10)
    assert result.decision.approved and result.status == "unknown"
    (row,) = await orders(sessions)
    assert (row.status, row.broker_order_id) == ("unknown", None)


class StuckGate:
    """Allows the gathering call; the pre-submit call hangs or raises."""

    def __init__(self, fail: str) -> None:
        self.fail = fail
        self.calls = 0

    async def status(self, now: datetime | None = None) -> ControlStatus:
        self.calls += 1
        if self.calls > 1:
            if self.fail == "hang":
                await hang()
            raise ConnectionError("control state unreadable")
        return ControlStatus(killed=False, reasons=())


@pytest.mark.parametrize("fail", ["hang", "raise"])
async def test_unanswered_pre_submit_gate_means_not_submitted(
    fail: str, sim: SimBroker, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    # 1.0 s: real-time headroom for the non-hanging calls; only the hung call should time out.
    engine = make_engine_(sim, sessions, StuckGate(fail), io_timeout_s=1.0, monotonic=Mono())
    result = await asyncio.wait_for(engine.check(intent(cycle_id)), 10)
    assert result.status == "not_submitted"
    (row,) = await orders(sessions)
    assert (row.status, row.broker_order_id) == ("not_submitted", None)


class HangingPositions(SimBroker):
    async def get_positions(self) -> list[Position]:
        await hang()


class HangingClose(SimBroker):
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        if order.side == "sell":
            await hang()
        return await super().submit_order(order)


async def test_hung_exit_state_rejects_and_hung_close_is_unknown(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    request = ExitRequest(symbol=BTC, reason="stop")
    # 1.0 s: real-time headroom for the non-hanging calls; only the hung call should time out.
    stuck = make_engine_(_sim(HangingPositions), sessions, gate, io_timeout_s=1.0, monotonic=Mono())
    rejected = await asyncio.wait_for(stuck.exit(request), 10)
    assert rejected.decision.reasons == ("state unavailable: TimeoutError",)
    assert await orders(sessions) == []

    broker = _sim(HangingClose)
    await _held_btc(broker)
    # 1.0 s: real-time headroom for the non-hanging calls; only the hung call should time out.
    closing = make_engine_(broker, sessions, gate, io_timeout_s=1.0, monotonic=Mono())
    result = await asyncio.wait_for(closing.exit(request), 10)
    assert result.decision.approved and result.status == "unknown"
    (row,) = await orders(sessions)
    assert (row.side, row.status, row.broker_order_id) == ("sell", "unknown", None)


# --- held positions without a trustworthy price ----------------------------------------

ETH = "ETH/USD"


async def _holding_eth() -> SimBroker:
    broker = SimBroker(
        cash=D("1000"),
        fees=FEES,
        slippage_bps=D(0),
        asset_rules={BTC: RULES, ETH: RULES},
        timeframe="5m",
    )
    for i in (29, 30):  # bar 29 prices both symbols, bar 30 fills the ETH market buy
        for symbol in (BTC, ETH):
            broker.process_bar(five(i).model_copy(update={"symbol": symbol}))
            if i == 29 and symbol == ETH:
                await broker.submit_order(
                    BrokerOrder(symbol=ETH, side="buy", qty=D("1"), type="market")
                )
    return broker


@pytest.mark.parametrize(
    "view",
    [
        FakeView(),  # the feed has a price for BTC only (Codex's reproduction)
        FakeView(prices={ETH: 100.0}, stale_symbols=frozenset({ETH})),  # ETH priced but stale
    ],
    ids=["unpriced", "stale"],
)
async def test_held_position_without_current_price_fails_entries_closed_not_exits(
    view: FakeView, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = await _holding_eth()
    clock = Clock(NOW)
    engine = make_engine_(broker, sessions, gate, view=view, clock=clock)
    result = await engine.check(intent(cycle_id))
    assert result.decision.reasons == (f"held position {ETH} has no current price",)
    assert await orders(sessions) == []

    clock.now = NOW + timedelta(minutes=1)
    await engine.snapshot_equity()  # informational exposure falls back to cost basis
    (position,) = await broker.get_positions()
    assert (await snapshots(sessions))[-1].exposure == position.qty * position.avg_entry_price

    exited = await engine.exit(ExitRequest(symbol=ETH, reason="stale"))
    assert exited.decision.approved and exited.status == "new"


async def test_exit_on_a_fully_reserved_holding_is_a_plain_failure(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    await _held_btc(sim)
    await sim.close_position(BTC)  # an open sell (not ours) reserves the whole holding
    result = await make_engine_(sim, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert not result.decision.approved
    assert result.decision.reasons == ("reserved by pending sells in BTC/USD",)
    assert (result.dust, result.no_position, result.order_row_id) == (False, False, None)
    assert await orders(sessions) == []


async def test_entry_exposure_counts_reserved_qty(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = await _holding_eth()  # 1 ETH
    await broker.close_position(ETH)  # a pending sell reserves all of it
    (pos,) = [p for p in await broker.get_positions() if p.symbol == ETH]
    assert pos.qty_available == 0
    view = FakeView(prices={ETH: 400.0})  # at its free qty ETH would count 0: entry fits
    result = await make_engine_(broker, sessions, gate, view=view).check(intent(cycle_id))
    assert not result.decision.approved and "exposure" in result.decision.reasons[0]


# --- ambiguous exits: a retry re-reads the position and cannot oversell -----------------


class LostCloseAck(SimBroker):
    """The broker accepts the close, but the ack never arrives (timeout after submit)."""

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        ack = await super().submit_order(order)
        if order.side == "sell":
            raise TimeoutError
        return ack


def sells(broker: SimBroker) -> list[str]:
    return [o.id for o in broker._orders.values() if o.order.side == "sell"]


@pytest.mark.parametrize(("filled", "retry_status"), [(True, None), (False, None)])
async def test_retried_unknown_exit_never_sells_twice(
    filled: bool,
    retry_status: str | None,
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
) -> None:
    broker = _sim(LostCloseAck)
    await _held_btc(broker)
    engine = make_engine_(broker, sessions, gate)
    request = ExitRequest(symbol=BTC, reason="stop")
    first = await engine.exit(request)
    assert first.status == "unknown" and first.broker_order_id is None
    if filled:
        broker.process_bar(five(31))  # the lost sell fills: the position is gone
    retry = await engine.exit(request)
    if filled:  # the retry re-reads the position and finds nothing to sell
        assert not retry.decision.approved and "no position" in retry.decision.reasons[0]
    else:  # still held, but the lost sell reserves it all: refused before any row
        assert retry.decision.reasons == ("reserved by pending sells in BTC/USD",)
    assert retry.status == retry_status
    assert len(sells(broker)) == 1
    rows = [o.status for o in await orders(sessions)]
    assert rows == ["unknown"]


# --- one shared deadline per locked section ---------------------------------------------


class Mono:
    """A monotonic clock the test advances (the engine's budget clock)."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class RecordingEngine(RiskEngine):
    """Records the budget left each time a step is handed to `_within`."""

    budgets: list[float]

    async def _within(self, deadline: float, call: Callable[[], Awaitable[T]]) -> T:
        self.budgets.append(deadline - self._monotonic())
        return await super()._within(deadline, call)


class SlowBroker(SimBroker):
    """Spends `spend` of the budget clock in `slow_call` ("get_account" or
    "get_positions"); submit_order hangs."""

    mono: Mono | None = None
    spend = 0.0
    slow_call = ""

    def _maybe_spend(self, call: str) -> None:
        if self.mono is not None and call == self.slow_call:
            self.mono.t += self.spend

    async def get_account(self) -> Account:
        self._maybe_spend("get_account")
        return await super().get_account()

    async def get_positions(self) -> list[Position]:
        self._maybe_spend("get_positions")
        return await super().get_positions()

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        if self.mono is not None:  # armed by the test; setup orders go through
            await hang()
        return await super().submit_order(order)


def _recording(
    broker: SimBroker,
    sessions: async_sessionmaker[AsyncSession],
    gate: Gate,
    mono: Mono,
    budget: float,
) -> RecordingEngine:
    engine = make_engine_(
        broker, sessions, gate, io_timeout_s=budget, monotonic=mono, cls=RecordingEngine
    )
    assert isinstance(engine, RecordingEngine)
    engine.budgets = []
    return engine


async def test_entry_submit_gets_only_what_the_gather_left(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    """Gather spends 0.6 of the budget, the submit hangs: the submit is handed the 0.4
    remainder, not a fresh budget, so the lock is held ~1 budget in total."""
    budget, mono = 0.05, Mono()
    broker = _sim(SlowBroker)
    assert isinstance(broker, SlowBroker)
    broker.mono, broker.spend, broker.slow_call = mono, 0.6 * budget, "get_account"
    engine = _recording(broker, sessions, gate, mono, budget)
    result = await engine.check(intent(cycle_id))
    assert result.status == "unknown"
    assert engine.budgets[0] == pytest.approx(budget)  # gate, the first gather step
    assert engine.budgets[-1] == pytest.approx(0.4 * budget)  # the submit: the remainder
    assert max(engine.budgets[1:]) <= budget  # never a fresh budget after the first step


async def test_exit_close_gets_only_what_the_position_read_left(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    budget, mono = 0.05, Mono()
    broker = _sim(SlowBroker)
    assert isinstance(broker, SlowBroker)
    await _held_btc(broker)
    broker.mono, broker.spend, broker.slow_call = mono, 0.6 * budget, "get_positions"
    engine = _recording(broker, sessions, gate, mono, budget)
    result = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))
    assert result.status == "unknown"
    assert engine.budgets[0] == pytest.approx(budget)  # the position read
    assert engine.budgets[-1] == pytest.approx(0.4 * budget)  # the close: the remainder


class BudgetEatingGate:
    """Allowed every time; the pre-submit call uses up the whole budget."""

    def __init__(self, mono: Mono) -> None:
        self.mono = mono
        self.calls = 0

    async def status(self, now: datetime | None = None) -> ControlStatus:
        self.calls += 1
        if self.calls == 2:
            self.mono.t += 100.0
        return ControlStatus(killed=False, reasons=())


async def test_budget_spent_before_submit_is_not_submitted_and_never_reaches_broker(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    mono = Mono()
    engine = make_engine_(sim, sessions, BudgetEatingGate(mono), io_timeout_s=10, monotonic=mono)
    result = await engine.check(intent(cycle_id))
    assert result.decision.approved and result.status == "not_submitted"
    (row,) = await orders(sessions)
    assert (row.status, row.broker_order_id) == ("not_submitted", None)
    assert sim._orders == {}  # nothing reached the broker


class BudgetEatingRules(SimBroker):
    """`asset_rules` (the last budgeted gather step of entries and exits) spends the
    whole budget once armed."""

    mono: Mono | None = None

    async def asset_rules(self, symbol: str) -> AssetRules:
        if self.mono is not None:
            self.mono.t += 100.0
        return await super().asset_rules(symbol)


async def test_budget_spent_by_gather_skips_the_gate_and_the_broker(
    sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    mono = Mono()
    broker = _sim(BudgetEatingRules)
    assert isinstance(broker, BudgetEatingRules)
    broker.mono = mono
    counting = BudgetEatingGate(Mono())  # its own clock: only counts calls here
    engine = make_engine_(broker, sessions, counting, io_timeout_s=10, monotonic=mono)
    result = await engine.check(intent(cycle_id))
    assert result.status == "not_submitted"
    assert counting.calls == 1  # gathering only: the pre-submit gate was never asked
    assert broker._orders == {}


async def test_exit_gather_and_close_share_one_budget(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    mono = Mono()
    broker = _sim(BudgetEatingRules)
    assert isinstance(broker, BudgetEatingRules)
    await _held_btc(broker)
    broker.mono = mono
    engine = make_engine_(broker, sessions, gate, io_timeout_s=10, monotonic=mono)
    result = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))
    assert result.decision.approved and result.status == "not_submitted"
    assert sells(broker) == []
    (row,) = await orders(sessions)
    assert (row.side, row.status) == ("sell", "not_submitted")


@pytest.mark.parametrize("slow", ["journal", "broker"])
async def test_only_network_calls_count_against_the_budget(
    slow: str,
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Journal work is never cancelled by the budget (a cancelled session can poison a
    pooled connection for the next caller, e.g. an exit); a slow broker call is."""
    budget = 1.0
    broker = _sim(HangingAccount) if slow == "broker" else _sim()
    if slow == "journal":
        original = repo.strategy_trade_stats

        async def slow_stats(session: AsyncSession, strategy_id: int) -> tuple[int, int]:
            await asyncio.sleep(2 * budget)
            return await original(session, strategy_id)

        monkeypatch.setattr(repo, "strategy_trade_stats", slow_stats)
    engine = make_engine_(broker, sessions, gate, io_timeout_s=budget, monotonic=Mono())
    result = await asyncio.wait_for(engine.check(intent(cycle_id)), 10)
    if slow == "journal":
        assert result.decision.approved and result.status == "new"
    else:
        assert result.decision.reasons == ("state unavailable: TimeoutError",)


# --- exits never wait for entries (separate locks) ------------------------------------


async def test_stalled_journal_read_in_an_entry_cannot_block_an_exit(
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex's reproduction: a journal await inside check() never returns (outside the io
    budget by design); a queued stop exit must still go through."""
    broker = _sim()
    await _held_btc(broker)
    stalled = asyncio.Event()

    async def never(session: AsyncSession, strategy_id: int) -> tuple[int, int]:
        stalled.set()
        await hang()

    monkeypatch.setattr(repo, "strategy_trade_stats", never)
    engine = make_engine_(broker, sessions, gate, monotonic=Mono())
    entry = asyncio.create_task(engine.check(intent(cycle_id)))
    try:
        await asyncio.wait_for(stalled.wait(), 2)  # the entry is stuck inside its section
        exited = await asyncio.wait_for(engine.exit(ExitRequest(symbol=BTC, reason="stop")), 2)
        assert exited.decision.approved and exited.status == "new"
        assert not entry.done()
    finally:
        entry.cancel()
        await asyncio.gather(entry, return_exceptions=True)


async def test_concurrent_entries_are_still_serialised(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate)
    results = await asyncio.gather(
        engine.check(intent(cycle_id)), engine.check(intent(await new_cycle(sessions)))
    )
    # whichever ran first (no order assumed): one submitted, the other saw it
    (won,) = [r for r in results if r.decision.approved]
    (lost,) = [r for r in results if not r.decision.approved]
    assert won.status == "new"
    assert "already has a position or open entry" in lost.decision.reasons[0]
    assert [o.status for o in await orders(sessions)] == ["new"]


async def test_concurrent_exits_on_one_symbol_sell_once(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    await _held_btc(sim)
    engine = make_engine_(sim, sessions, gate)
    request = ExitRequest(symbol=BTC, reason="stop")
    results = await asyncio.gather(engine.exit(request), engine.exit(request))
    # the first sell reserves the holding: the second exit is refused before any row
    assert sorted(str(r.status) for r in results) == ["None", "new"]
    (lost,) = [r for r in results if r.status is None]
    assert lost.decision.reasons == ("reserved by pending sells in BTC/USD",)
    assert len(sells(sim)) == 1
    assert len(await orders(sessions)) == 1


class AccountDown(SimBroker):
    async def get_account(self) -> Account:
        raise ConnectionError("account down")


class PositionsDown(SimBroker):
    async def get_positions(self) -> list[Position]:
        raise ConnectionError("positions down")


def _broker(cls: type[SimBroker]) -> SimBroker:
    b = cls(cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m")
    b.process_bar(five(29))
    return b


async def test_last_snapshot_at_tracks_committed_writes(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    mono = Mono()
    mono.t = 2.0
    ok = make_engine_(_broker(SimBroker), sessions, gate, monotonic=mono, healthy=False)
    assert ok.last_snapshot_at is None
    await ok.snapshot_equity()
    assert ok.last_snapshot_at == 2.0

    no_account = make_engine_(_broker(AccountDown), sessions, gate, monotonic=mono, healthy=False)
    with pytest.raises(ConnectionError):
        await no_account.snapshot_equity()
    assert no_account.last_snapshot_at is None  # nothing was written

    mono.t = 5.0  # advance, so the assertion shows the value was set by this write
    later = Clock(NOW + timedelta(minutes=1))  # equity_snapshots.ts is unique
    partial = make_engine_(
        _broker(PositionsDown), sessions, gate, clock=later, monotonic=mono, healthy=False
    )
    with pytest.raises(ConnectionError):
        await partial.snapshot_equity()
    assert partial.last_snapshot_at == 5.0  # equity/HWM still committed


def spy_portfolios(monkeypatch: pytest.MonkeyPatch) -> list[PortfolioState]:
    seen: list[PortfolioState] = []
    real = check_entry  # mypy: risk.engine does not re-export it

    def spy(
        intent: EntryIntent, portfolio: PortfolioState, market: MarketSnapshot, **kw: Any
    ) -> RiskDecision:
        seen.append(portfolio)
        return real(intent, portfolio, market, **kw)

    monkeypatch.setattr(engine_module, "check_entry", spy)
    return seen


async def test_a_partially_filled_entry_reserves_only_its_remainder(
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _sim(PartialFillBroker)  # reports the canceled IOC as still partially filled
    broker.script(BTC, "buy", FillPlan((D("0.5"),)))
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))  # 2.997 at limit 100.10
    broker.process_bar(five(30))  # fills 1.4985
    seen = spy_portfolios(monkeypatch)

    await engine.check(intent(await new_cycle(sessions)))
    assert seen[-1].open_entries == (
        OpenEntry(symbol=BTC, notional=(D("2.997") - D("1.4985")) * D("100.10")),
        OpenEntry(symbol=BTC, notional=D(0)),  # the open journal trade (booked fill)
    )


class HiddenFillsBroker(SimBroker):
    """Reports the order final but none of its executions yet (visibility lag)."""

    async def order_fills(self, order_id: str) -> list[Fill]:
        return []


async def test_a_final_unsettled_entry_reserves_nothing_but_blocks_its_symbol(
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _sim(HiddenFillsBroker)
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))
    broker.process_bar(five(30))  # filled at the broker, fills not visible
    seen = spy_portfolios(monkeypatch)

    result = await engine.check(intent(await new_cycle(sessions)))
    assert seen[-1].open_entries == (OpenEntry(symbol=BTC, notional=D(0)),)
    assert "already has a position or open entry" in result.decision.reasons[0]
    first = (await orders(sessions))[0]
    assert (first.status, first.settled_at) == ("filled", None)


async def test_a_canceled_entry_with_hidden_fills_reserves_nothing_but_blocks_its_symbol(
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Spec 4.7: a final unsettled row has notional 0, even when the broker's filled_qty
    # (here 1.4985 of 2.997) leaves an unfilled remainder: a canceled IOC never fills it.
    broker = _sim(HiddenFillsBroker)
    broker.script(BTC, "buy", FillPlan((D("0.5"),)))
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))
    broker.process_bar(five(30))  # half fills, the IOC remainder is canceled; fills hidden
    seen = spy_portfolios(monkeypatch)

    result = await engine.check(intent(await new_cycle(sessions)))
    assert seen[-1].open_entries == (OpenEntry(symbol=BTC, notional=D(0)),)
    assert "already has a position or open entry" in result.decision.reasons[0]
    first = (await orders(sessions))[0]
    assert (first.status, first.settled_at) == ("canceled", None)


async def test_a_held_back_local_final_entry_reserves_nothing(
    sim: SimBroker,
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A not_submitted row behind an earlier id-less pending row on its symbol is deferred by
    # the ledger (stays unsettled): it still blocks ETH but must not reserve notional.
    async with sessions() as s:
        for status in ("pending", "not_submitted"):
            await repo.insert_order(
                s,
                cycle_id=None,
                client_order_id=None,
                symbol="ETH/USD",
                mode="paper",
                type_="limit",
                side="buy",
                qty=D("1"),
                limit_price=D("10"),
                stop_price=None,
                tp_price=None,
                status=status,
                now=NOW,
            )
    seen = spy_portfolios(monkeypatch)

    await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert seen[-1].open_entries == (
        OpenEntry(symbol="ETH/USD", notional=D("10")),  # pending: full reservation
        OpenEntry(symbol="ETH/USD", notional=D(0)),  # not_submitted, held back
    )
    assert [o.settled_at for o in await orders(sessions)][:2] == [None, None]


async def test_an_unreadable_order_fails_entries_closed_but_the_rest_is_booked(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate)
    await engine.check(intent(cycle_id))
    sim.process_bar(five(30))  # the BTC entry fills
    async with sessions() as s:  # an order the broker no longer knows, on another symbol
        ghost = await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id=None,
            symbol="ETH/USD",
            mode="paper",
            type_="limit",
            side="buy",
            qty=D("1"),
            limit_price=D("10"),
            stop_price=None,
            tp_price=None,
            status="pending",
            now=NOW,
        )
        await repo.update_order(s, ghost.id, status="new", now=NOW, broker_order_id="sim-404")

    result = await engine.check(intent(await new_cycle(sessions)))
    assert result.decision.reasons == ("state unavailable: SyncIncomplete",)
    async with sessions() as s:
        (pos,) = list(await s.scalars(select(m.Position)))
    assert pos.qty == D("2.997")  # the readable order was still booked
    exited = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))  # exits never sync
    assert exited.decision.approved and exited.status == "new"


class FailingLedger:
    async def apply_all(self, result: SyncResult) -> frozenset[str]:
        raise RuntimeError("journal down")


class OneSymbolFailed:
    async def apply_all(self, result: SyncResult) -> frozenset[str]:
        return frozenset({"ETH/USD"})


async def test_a_symbol_the_ledger_could_not_book_fails_every_entry_closed(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate, ledger=OneSymbolFailed())
    result = await engine.check(intent(cycle_id))  # a BTC entry
    assert result.decision.reasons == ("state unavailable: SyncIncomplete",)


async def test_a_failing_ledger_fails_entries_closed(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    result = await make_engine_(sim, sessions, gate, ledger=FailingLedger()).check(intent(cycle_id))
    assert result.decision.reasons == ("state unavailable: RuntimeError",)


ETH = "ETH/USD"


class OverfilledEthBroker(SimBroker):
    """Reports a journal ETH buy of qty 1 as filled 2: a ledger anomaly (`overfilled`)."""

    async def get_order(self, order_id: str) -> OrderStatus:
        if order_id != "eth-1":
            return await super().get_order(order_id)
        return OrderStatus(
            order_id=order_id, status="filled", filled_qty=D("2"), avg_fill_price=D("10")
        )

    async def order_fills(self, order_id: str) -> list[Fill]:
        if order_id != "eth-1":
            return await super().order_fills(order_id)
        return [
            Fill(
                fill_id="eth-f1",
                order_id=order_id,
                symbol=ETH,
                side="buy",
                qty=D("2"),
                price=D("10"),
                fee=D("0.05"),
                fee_asset="quote",
                ts=NOW,
            )
        ]

    async def asset_rules(self, symbol: str) -> AssetRules:
        return RULES if symbol == ETH else await super().asset_rules(symbol)


async def test_an_anomaly_found_by_this_checks_sync_rejects_the_entry_as_paused(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    # The gate is read after the ledger booked the sync: a pause set by this very pass
    # rejects the entry, so no approved decision and no pending order row are written.
    broker = _sim(OverfilledEthBroker)
    async with sessions() as s:
        eth = await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id=None,
            symbol=ETH,
            mode="paper",
            type_="limit",
            side="buy",
            qty=D("1"),
            limit_price=D("10"),
            stop_price=None,
            tp_price=None,
            status="pending",
            now=NOW,
        )
        await repo.update_order(s, eth.id, status="new", now=NOW, broker_order_id="eth-1")

    result = await make_engine_(broker, sessions, gate).check(intent(cycle_id))
    assert not result.decision.approved
    assert result.decision.reasons[0].startswith("control: trading paused")
    assert [o.id for o in await orders(sessions)] == [eth.id]  # no pending BTC row
    (dec,) = await decisions(sessions)
    assert not dec.approved


async def test_sync_orders_reads_every_unsettled_row_once(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate)
    await engine.check(intent(cycle_id))  # row 1: acked "new", nothing filled yet
    direct = await sim.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=D("0.01"), type="market")
    )
    async with sessions() as s:

        async def row(status: str, broker_id: str | None) -> int:
            r = await repo.insert_order(
                s,
                cycle_id=None,
                client_order_id=None,
                symbol=BTC,
                mode="paper",
                type_="market",
                side="buy",
                qty=D("0.01"),
                limit_price=None,
                stop_price=None,
                tp_price=None,
                status=status,
                now=NOW,
            )
            if broker_id is not None:
                await repo.update_order(s, r.id, status=status, now=NOW, broker_order_id=broker_id)
            return r.id

        unknown = await row("unknown", None)  # the resolver's type (a): not read
        never = await row("not_submitted", None)  # local final: no broker call
        ghost = await row("new", "sim-404")  # the broker does not know it
        same_symbol = await row("new", direct.order_id)  # second BTC row: rules cached

    # The read step alone (check()'s) reports the ghost as unreadable ...
    assert (await engine._sync(time.monotonic() + 10)).failed == (ghost,)
    # ... the full pass resolves it: NULL client ids carry no ownership proof, so the ghost
    # and the id-less unknown row become incidents, and an incident leaves `failed`.
    first = await engine.sync_orders()
    assert first.failed == ()
    assert [(x.order_row_id, x.action, x.reason) for x in first.resolutions] == [
        (unknown, "incident", "no_ownership_proof"),
        (ghost, "start_clock", None),
        (ghost, "incident", "no_ownership_proof"),
    ]
    by_row = {u.order_row_id: u for u in first.updates}
    assert unknown not in by_row
    assert (by_row[never].status, by_row[never].min_qty) == ("not_submitted", None)
    assert by_row[1].fills == () and by_row[1].min_qty == RULES.min_qty
    sim.process_bar(five(30))
    second = await engine.sync_orders()
    filled = {u.order_row_id: u for u in second.updates}
    assert len(filled[1].fills) == 1 and len(filled[same_symbol].fills) == 1


class DustBroker(SimBroker):
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        if order.side == "sell":
            raise DustRemainder(order.symbol, D("0.00005"), RULES.min_qty)
        return await super().submit_order(order)


async def test_a_dust_close_is_reported_as_dust(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    broker = _sim(DustBroker)
    await _held_btc(broker)
    result = await make_engine_(broker, sessions, gate).exit(ExitRequest(symbol=BTC, reason="kill"))
    assert (result.status, result.dust) == ("rejected", True)
    assert result.min_qty == RULES.min_qty
    (row,) = await orders(sessions)
    assert (row.status, row.exit_reason) == ("rejected", "kill")


async def test_exit_outcomes_say_whether_the_broker_held_dust_or_nothing(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    nothing = await make_engine_(_sim(), sessions, gate).exit(
        ExitRequest(symbol=BTC, reason="stop")
    )
    assert (nothing.no_position, nothing.dust, nothing.min_qty) == (True, False, None)

    dusty = SimBroker(  # base fee: a 0.0001 buy nets 0.00009975 < min_qty
        cash=D("1000"),
        fees=FeeSchedule(maker_bps=D(15), taker_bps=D(25), buy_fee_asset="base"),
        slippage_bps=D(0),
        asset_rules={BTC: RULES.model_copy(update={"min_notional": D(0)})},  # 0.01 USD buy
        timeframe="5m",
    )
    dusty.process_bar(five(29))
    await dusty.submit_order(BrokerOrder(symbol=BTC, side="buy", qty=D("0.0001"), type="market"))
    dusty.process_bar(five(30))
    dust = await make_engine_(dusty, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert (dust.no_position, dust.dust, dust.min_qty) == (False, True, RULES.min_qty)
    assert dust.decision.reasons == ("dust only in BTC/USD",) and dust.order_row_id is None
    assert await make_engine_(dusty, sessions, gate).sellable_positions() == ()


async def test_a_mode_mismatch_is_neither_dust_nor_no_position(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    await _held_btc(sim)
    sim.broker_mode = "live"
    result = await make_engine_(sim, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert (result.dust, result.no_position, result.min_qty) == (False, False, None)


async def test_an_open_journal_trade_blocks_its_symbol_even_on_broker_dust(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    # PR #25 review: a trade kept open by a lagging exit while the broker holds only dust
    # (which check 5 ignores) must still block a second trade on the symbol.
    async with sessions() as s:
        s.add(
            m.Position(
                symbol=BTC,
                side="long",
                qty=D("0.00005"),
                entry_price=D("100"),
                opened_at=NOW,
                fees=D(0),
                dust_consumed=D(0),
            )
        )
        await s.commit()
    result = await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert "already has a position or open entry" in result.decision.reasons[0]


async def test_snapshot_equity_returns_the_account_it_read(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    account = await make_engine_(sim, sessions, gate).snapshot_equity()
    assert account.cash == D("1000")


class UnavailableBroker(SimBroker):
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        if order.side == "sell":
            raise OrderMethodsUnavailable("AlpacaBroker.submit_order is not implemented until 1.6")
        return await super().submit_order(order)

    def can_place_orders(self) -> bool:
        return False


async def test_an_exit_on_a_broker_that_cannot_trade_is_not_submitted(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    broker = _sim(UnavailableBroker)
    await _held_btc(broker)
    engine = make_engine_(broker, sessions, gate)
    assert engine.can_place_orders() is False
    result = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))
    assert (result.status, result.broker_order_id) == ("not_submitted", None)
    (row,) = await orders(sessions)
    assert row.status == "not_submitted"  # never "unknown": nothing left the process


async def test_an_external_exit_marks_its_order_row(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    await _held_btc(sim)
    result = await make_engine_(sim, sessions, gate).exit(
        ExitRequest(symbol=BTC, reason="flatten", external=True)
    )
    assert result.status == "new"
    (row,) = await orders(sessions)
    assert (row.external, row.exit_reason, row.cycle_id) == (True, "flatten", None)


class CancelSpy(SimBroker):
    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self.cancelled: list[str] = []
        self.fail: set[str] = set()

    async def cancel_order(self, order_id: str) -> None:
        if order_id in self.fail:
            raise OrderRejected("cannot cancel")
        self.cancelled.append(order_id)
        await super().cancel_order(order_id)


async def _entry_row(
    sessions: async_sessionmaker[AsyncSession],
    status: str,
    broker_id: str | None,
    side: str = "buy",
) -> int:
    async with sessions() as s:
        row = await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id=None,
            symbol=BTC,
            mode="paper",
            type_="limit",
            side=side,
            qty=D("1"),
            limit_price=D("50"),
            stop_price=None,
            tp_price=None,
            status="pending",
            now=NOW,
        )
        await repo.update_order(s, row.id, status=status, now=NOW, broker_order_id=broker_id)
        return row.id


async def test_cancel_open_entries_cancels_open_buys_only(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    broker = _sim(CancelSpy)
    assert isinstance(broker, CancelSpy)
    await _held_btc(broker)
    resting = await broker.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=D("1"), type="limit", limit_price=D("50"))
    )
    failing = await broker.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=D("1"), type="limit", limit_price=D("51"))
    )
    broker.fail.add(failing.order_id)
    sell = await broker.close_position(BTC)
    open_row = await _entry_row(sessions, "new", resting.order_id)
    failed_row = await _entry_row(sessions, "new", failing.order_id)
    unacked = await _entry_row(sessions, "unknown", None)
    await _entry_row(sessions, "not_submitted", None)  # final: skipped
    await _entry_row(sessions, "new", sell.order_id, side="sell")  # never a sell
    skipped = await _entry_row(sessions, "new", "sim-999")
    engine = make_engine_(broker, sessions, gate)

    result = await engine.cancel_open_entries(exclude={skipped})

    assert result == CancelResult(
        requested=(open_row,), failed=(failed_row,), unresolved=(unacked,)
    )
    assert broker.cancelled == [resting.order_id]
    assert (await broker.get_order(sell.order_id)).status == "new"


async def test_cancel_open_entries_with_a_spent_budget_fails_the_row(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    row = await _entry_row(sessions, "new", "sim-1")
    engine = make_engine_(sim, sessions, gate, io_timeout_s=0.0)
    assert await engine.cancel_open_entries() == CancelResult(failed=(row,))


async def test_sellable_positions_leave_out_dust(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    engine = make_engine_(sim, sessions, gate)
    assert await engine.sellable_positions() == ()
    await _held_btc(sim)
    (held,) = await engine.sellable_positions()
    assert (held.symbol, held.qty, held.market_value) == (BTC, D("1"), None)


# --- trusted daily-loss baseline, unsettled sells, receipt-time stamps (WP 1.5d-1) ------

DAY_START = day_start(NOW, "UTC")
BASELINE_UNAVAILABLE = ("daily-loss baseline unavailable",)


async def _snapshot(
    sessions: async_sessionmaker[AsyncSession], ts: datetime, equity: str = "1000"
) -> None:
    async with sessions() as s:
        await repo.insert_equity_snapshot(
            s,
            ts=ts,
            equity=D(equity),
            cash=D(equity),
            exposure=D(0),
            hwm=D(equity),
            daily_pnl=D(0),
        )


@pytest.mark.no_baseline
async def test_gapped_day_boundary_rejects_baseline_unavailable(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    await _snapshot(sessions, DAY_START - timedelta(hours=3), "1000")  # stale pre-start
    await _snapshot(sessions, DAY_START + timedelta(hours=1), "1100")  # first post-start
    broker = _sim(EquityBroker)
    assert isinstance(broker, EquityBroker)
    broker.equity = D("1080")
    result = await make_engine_(broker, sessions, gate).check(intent(cycle_id))
    assert result.decision.reasons == BASELINE_UNAVAILABLE
    assert await orders(sessions) == []


@pytest.mark.no_baseline
async def test_no_pre_start_snapshot_rejects_even_with_later_snapshots(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    for minutes in range(1, 6):
        await _snapshot(sessions, DAY_START + timedelta(minutes=minutes))
    result = await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert result.decision.reasons == BASELINE_UNAVAILABLE
    assert await orders(sessions) == []


@pytest.mark.no_baseline
@pytest.mark.parametrize(("minute", "approved"), [(59, True), (57, False)])
async def test_berlin_baseline_window_is_relative_to_the_local_day_start(
    minute: int,
    approved: bool,
    sim: SimBroker,
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
) -> None:
    assert day_start(NOW, "Europe/Berlin") == datetime(2026, 10, 8, 22, 0, tzinfo=UTC)
    await _snapshot(sessions, datetime(2026, 10, 8, 21, minute, tzinfo=UTC))
    engine = make_engine_(sim, sessions, gate, tz="Europe/Berlin")
    result = await engine.check(intent(cycle_id))
    assert result.decision.approved is approved
    if not approved:
        assert result.decision.reasons == BASELINE_UNAVAILABLE


@pytest.mark.no_baseline
async def test_next_day_with_fresh_snapshot_allows_entries(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    clock = Clock(NOW)
    engine = make_engine_(sim, sessions, gate, clock=clock)
    day_n = await engine.check(intent(cycle_id))
    assert day_n.decision.reasons == BASELINE_UNAVAILABLE
    next_start = next_day_start(NOW, "UTC")
    await _snapshot(sessions, next_start - timedelta(seconds=30))
    clock.now = next_start + timedelta(hours=12)
    day_n1 = await engine.check(intent(await new_cycle(sessions)))
    assert day_n1.decision.approved and day_n1.status == "new"


@pytest.mark.no_baseline
async def test_decision_never_uses_the_untrusted_day_start_equity(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    await _snapshot(sessions, DAY_START + timedelta(minutes=1), "1000")  # == current equity
    async with sessions() as s:
        assert await repo.day_start_equity(s, DAY_START) == D("1000")
        assert await repo.trusted_day_start_equity(s, DAY_START, timedelta(seconds=120)) is None
    result = await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert result.decision.reasons == BASELINE_UNAVAILABLE
    assert await orders(sessions) == []


async def test_unsettled_external_sell_blocks_its_symbol(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    # Id-less and pending: the gather's own sync skips it (a final row would be settled).
    async with sessions() as s:
        sell = await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id=None,
            symbol=BTC,
            mode="paper",
            type_="market",
            side="sell",
            qty=D("1"),
            limit_price=None,
            stop_price=None,
            tp_price=None,
            status="pending",
            now=NOW,
            external=True,
        )
    engine = make_engine_(sim, sessions, gate)
    blocked = await engine.check(intent(cycle_id))
    assert blocked.decision.reasons == ("unsettled sell on BTC/USD",)
    async with sessions() as s:
        await s.execute(update(m.Order).where(m.Order.id == sell.id).values(settled_at=NOW))
        await s.commit()
    allowed = await engine.check(intent(await new_cycle(sessions)))
    assert allowed.decision.approved and allowed.status == "new"


class CrossingBroker(SimBroker):
    """`get_account` moves the engine clock to `to` before it returns (a slow read that
    crosses the day boundary)."""

    clock: Clock | None = None
    to: datetime | None = None

    async def get_account(self) -> Account:
        account = await super().get_account()
        if self.clock is not None and self.to is not None:
            self.clock.now = self.to
        return account


@pytest.mark.no_baseline
@pytest.mark.parametrize("writer", ["loop", "check"])
async def test_account_read_crossing_midnight_is_stamped_at_receipt(
    writer: str, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    boundary = next_day_start(NOW, "UTC")
    broker = _sim(CrossingBroker)
    assert isinstance(broker, CrossingBroker)
    clock = Clock(boundary - timedelta(seconds=1))
    broker.clock, broker.to = clock, boundary + timedelta(seconds=1)
    engine = make_engine_(broker, sessions, gate, clock=clock)
    if writer == "loop":
        await engine.snapshot_equity()
        clock.now = boundary + timedelta(seconds=1)
        result = await engine.check(intent(cycle_id))
    else:  # empty journal: the first check writes the peak snapshot
        result = await engine.check(intent(cycle_id))
    ((row),) = await snapshots(sessions)
    assert row.ts == boundary + timedelta(seconds=1)
    assert result.decision.reasons == BASELINE_UNAVAILABLE


@pytest.mark.no_baseline
async def test_exits_and_cancels_work_on_a_baseline_unavailable_day(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = _sim(CancelSpy)
    assert isinstance(broker, CancelSpy)
    await _held_btc(broker)
    resting = await broker.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=D("1"), type="limit", limit_price=D("50"))
    )
    row_id = await _entry_row(sessions, "new", resting.order_id)
    engine = make_engine_(broker, sessions, gate)
    assert (await engine.check(intent(cycle_id))).decision.reasons == BASELINE_UNAVAILABLE

    cancelled = await engine.cancel_open_entries()
    assert cancelled.requested == (row_id,) and broker.cancelled == [resting.order_id]

    result = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))
    assert result.decision.approved and result.status == "new"


async def test_peak_snapshot_in_check_does_not_refresh_last_snapshot_at(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = _sim(EquityBroker)
    assert isinstance(broker, EquityBroker)
    broker.equity = D("1100")  # above the seeded HWM 1000: check() writes a peak row
    mono = Mono()
    mono.t = 3.0
    engine = make_engine_(broker, sessions, gate, monotonic=mono, healthy=False)
    await engine.check(intent(cycle_id))
    assert [r.equity for r in await snapshots(sessions)] == [D("1000"), D("1100")]
    assert engine.last_snapshot_at is None


# --- health faults: snapshot freshness and the cash-reconciliation fault ----------------


async def reasons_of(engine: RiskEngine, cycle_id: int) -> tuple[str, ...]:
    result = await engine.check(intent(cycle_id))
    return result.decision.reasons


async def test_fresh_engine_rejects_snapshot_overdue_first(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate, healthy=False)
    result = await engine.check(intent(cycle_id))
    assert not result.decision.approved
    assert result.decision.reasons == ("equity snapshot overdue",)
    assert result.decision.pause is None
    assert engine.health_faults() == ("equity snapshot overdue", "cash reconciliation failed")


async def test_loop_snapshot_older_than_two_intervals_rejects(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    mono = Mono()
    engine = make_engine_(_broker(SimBroker), sessions, gate, monotonic=mono, healthy=False)
    await engine.snapshot_equity()  # t = 0
    engine.cash_reconciled()
    mono.t = 120.0
    assert (await engine.check(intent(cycle_id))).decision.approved
    mono.t = 121.0
    assert await reasons_of(engine, cycle_id) == ("equity snapshot overdue",)


async def test_startup_cash_fault_rejects_until_reconciled(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate, healthy=False)
    engine.last_snapshot_at = time.monotonic()
    assert engine.cash_fault == "not yet reconciled"
    assert await reasons_of(engine, cycle_id) == ("cash reconciliation failed",)
    engine.cash_reconciled()
    assert engine.cash_fault is None
    assert (await engine.check(intent(cycle_id))).decision.approved


async def test_sticky_cash_fault_survives_reconciliation(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate, healthy=False)
    engine.last_snapshot_at = time.monotonic()
    engine.set_cash_fault("CommitUncertain", sticky=True)
    engine.cash_reconciled()
    assert engine.cash_fault == "CommitUncertain" and engine.cash_fault_sticky
    assert await reasons_of(engine, cycle_id) == ("cash reconciliation failed",)

    other = make_engine_(sim, sessions, gate, healthy=False)
    other.last_snapshot_at = time.monotonic()
    other.set_cash_fault("x")
    assert other.cash_fault == "x"
    other.cash_reconciled()
    assert other.cash_fault is None
    assert (await other.check(intent(cycle_id))).decision.approved


async def test_plain_fault_never_downgrades_a_sticky_one(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate, healthy=False)
    engine.last_snapshot_at = time.monotonic()
    engine.set_cash_fault("a", sticky=True)
    engine.set_cash_fault("b")
    engine.cash_reconciled()
    assert engine.cash_fault == "a" and engine.cash_fault_sticky
    assert await reasons_of(engine, cycle_id) == ("cash reconciliation failed",)


async def test_restart_with_fresh_journal_baseline_still_waits_for_loop_and_cash(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    # The autouse baseline is a trusted pre-boundary snapshot in the journal; a new engine
    # (a restart) still needs its own loop snapshot and a reconciled cash step.
    engine = make_engine_(_broker(SimBroker), sessions, gate, healthy=False)
    assert await reasons_of(engine, cycle_id) == ("equity snapshot overdue",)
    await engine.snapshot_equity()
    assert await reasons_of(engine, cycle_id) == ("cash reconciliation failed",)
    engine.cash_reconciled()
    assert (await engine.check(intent(cycle_id))).decision.approved


async def test_fault_transitions_are_logged_once(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    engine = make_engine_(sim, sessions, gate, healthy=False)
    with capture_logs() as logs:
        engine.set_cash_fault("first")
        engine.set_cash_fault("second")
        engine.cash_reconciled()
        engine.cash_reconciled()
        engine.set_cash_fault("third")
        engine.set_cash_fault("third", sticky=True)
        engine.set_cash_fault("third", sticky=True)
    set_ = [e for e in logs if e["event"] == "risk.cash_fault_set"]
    cleared = [e for e in logs if e["event"] == "risk.cash_fault_cleared"]
    assert len(cleared) == 1 and cleared[0]["log_level"] == "info"
    # A fresh engine already carries `not yet reconciled`: "first"/"second" are no transition.
    assert [(e["reason"], e["sticky"], e["log_level"]) for e in set_] == [
        ("third", False, "warning"),
        ("third", True, "warning"),
    ]


# --- pre-submit health re-check (spec 2026-11-09 6.1, cases 14a-14d) ----------------------


class RecheckGate:
    """Allows every call; the second one (check()'s pre-submit re-check) first awaits
    `on_recheck`, so the test acts after the gather, decision and order-row insert."""

    def __init__(self) -> None:
        self.calls = 0
        self.on_recheck: Callable[[], Awaitable[None]] | None = None

    async def status(self, now: datetime | None = None) -> ControlStatus:
        self.calls += 1
        if self.calls == 2 and self.on_recheck is not None:
            await self.on_recheck()
        return ControlStatus(killed=False, reasons=())


class BlockingInsertEngine(RiskEngine):
    """Once the entry decision is persisted, arms `hook.block_before`: the next commit, the
    order-row insert, waits for `release`."""

    hook: CommitHook
    release: asyncio.Event

    async def _persist(
        self, cycle_id: int | None, kind: str, decision: RiskDecision, now: datetime
    ) -> None:
        await super()._persist(cycle_id, kind, decision, now)
        if kind == "entry":
            self.hook.block_before = self.release


LATE_CASES = ["cash_fault", "sticky_cash_fault", "order_insert", "snapshot_age", "trading_day"]
LATE_REASON = {
    "cash_fault": "cash reconciliation failed",
    "sticky_cash_fault": "cash reconciliation failed",
    "order_insert": "cash reconciliation failed",
    "snapshot_age": "equity snapshot overdue",
    "trading_day": "trading day changed",
}


@dataclass
class LateRun:
    result: OrderResult
    exited: OrderResult | None
    logs: list[MutableMapping[str, Any]]
    btc_orders: list[str]  # BTC order ids the broker received


async def _late_fault(case: str, db_url: str, cycle_id: int, *, exit_meanwhile: bool) -> LateRun:
    """A healthy entry gather and decision; then `case` turns one health input bad before
    the submit (at the pre-submit gate call, or while the order-row insert commit waits).
    With `exit_meanwhile`, an ETH exit (held and priced, so check 8 passes) runs to its end
    while the entry waits at that barrier."""
    db = make_engine(db_url)
    hook = CommitHook()
    sessions = hooked_sessionmaker(db, hook)
    try:
        broker = await _holding_eth()
        gate = RecheckGate()
        mono = Mono()
        start = datetime(2026, 10, 9, 23, 59, 59, tzinfo=UTC) if case == "trading_day" else NOW
        clock = Clock(start)
        engine = make_engine_(
            broker,
            sessions,
            gate,
            view=FakeView(prices={ETH: 100.0}),
            clock=clock,
            monotonic=mono,
            cls=BlockingInsertEngine if case == "order_insert" else RiskEngine,
        )
        if case == "snapshot_age":
            mono.t = 119.0  # the loop snapshot (at t=0) is 119 s old at the gather
        exits: list[OrderResult] = []

        async def exit_eth() -> None:
            if exit_meanwhile:
                exits.append(await engine.exit(ExitRequest(symbol=ETH, reason="stop")))

        async def turn_bad() -> None:
            if case == "cash_fault":
                engine.set_cash_fault("cash mismatch")
            elif case == "sticky_cash_fault":
                engine.set_cash_fault("CommitUncertain", sticky=True)
            elif case == "snapshot_age":
                mono.t = 121.0
            elif case == "trading_day":
                clock.now = datetime(2026, 10, 10, 0, 0, 1, tzinfo=UTC)
            await exit_eth()

        with capture_logs() as logs:
            if case == "order_insert":
                assert isinstance(engine, BlockingInsertEngine)
                engine.hook, engine.release = hook, asyncio.Event()
                entry = asyncio.create_task(engine.check(intent(cycle_id)))
                try:
                    await asyncio.wait_for(hook.reached.wait(), 2)  # the insert commit waits
                    engine.set_cash_fault("cash mismatch")
                    await asyncio.wait_for(exit_eth(), 10)  # completes while it waits
                    assert not entry.done()
                    engine.release.set()
                    result = await asyncio.wait_for(entry, 2)
                finally:
                    entry.cancel()
                    await asyncio.gather(entry, return_exceptions=True)
            else:
                gate.on_recheck = turn_bad
                result = await asyncio.wait_for(engine.check(intent(cycle_id)), 10)
        btc = [o.id for o in broker._orders.values() if o.order.symbol == BTC]
        return LateRun(result, exits[0] if exits else None, logs, btc)
    finally:
        await db.dispose()


async def _assert_not_submitted(
    run: LateRun, case: str, sessions: async_sessionmaker[AsyncSession]
) -> None:
    assert run.result.decision.approved and run.result.status == "not_submitted"
    assert run.result.broker_order_id is None
    assert run.btc_orders == []  # nothing reached the broker
    (row,) = [o for o in await orders(sessions) if o.symbol == BTC]
    assert (row.status, row.broker_order_id) == ("not_submitted", None)
    assert run.result.order_row_id == row.id
    (dec,) = [d for d in await decisions(sessions) if d.kind == "entry"]
    assert dec.approved  # the approved decision stays persisted
    (late,) = [e for e in run.logs if e["event"] == "risk.entry_not_submitted"]
    assert late["reasons"] == [LATE_REASON[case]]


@pytest.mark.parametrize("sticky", [False, True])
async def test_cash_fault_set_after_gather_stops_the_submit(
    sticky: bool, db_url: str, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    case = "sticky_cash_fault" if sticky else "cash_fault"
    run = await _late_fault(case, db_url, cycle_id, exit_meanwhile=False)
    await _assert_not_submitted(run, case, sessions)


async def test_cash_fault_set_during_order_insert_stops_the_submit(
    db_url: str, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    run = await _late_fault("order_insert", db_url, cycle_id, exit_meanwhile=False)
    await _assert_not_submitted(run, "order_insert", sessions)


async def test_snapshot_ageing_past_the_bound_during_gather_stops_the_submit(
    db_url: str, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    run = await _late_fault("snapshot_age", db_url, cycle_id, exit_meanwhile=False)
    await _assert_not_submitted(run, "snapshot_age", sessions)


async def test_trading_day_change_between_gather_and_submit_stops_the_submit(
    db_url: str, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    run = await _late_fault("trading_day", db_url, cycle_id, exit_meanwhile=False)
    await _assert_not_submitted(run, "trading_day", sessions)


@pytest.mark.parametrize("case", LATE_CASES)
async def test_exit_goes_through_while_entry_submit_is_blocked(
    case: str, db_url: str, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    run = await _late_fault(case, db_url, cycle_id, exit_meanwhile=True)
    assert run.exited is not None
    assert run.exited.decision.approved and run.exited.status == "new"
    await _assert_not_submitted(run, case, sessions)
    (sell,) = [o for o in await orders(sessions) if o.side == "sell"]
    assert (sell.symbol, sell.status) == (ETH, "new")
