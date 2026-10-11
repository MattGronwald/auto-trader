"""RiskEngine on SimBroker, a migrated SQLite journal and the real ControlGate."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import NoReturn

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import (
    Account,
    AssetRules,
    Broker,
    BrokerOrder,
    FeeSchedule,
    OrderAck,
    OrderRejected,
    OrderStatus,
    Position,
)
from autotrader.broker.sim import SimBroker
from autotrader.core.config import StrategyProfile, load_profile
from autotrader.core.kill import (
    PAUSED_KEY,
    PAUSED_UNTIL_KEY,
    ControlGate,
    ControlStatus,
    write_kill_file,
)
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import Gate, RiskEngine
from autotrader.risk.types import EntryIntent, ExitRequest

REPO = Path(__file__).resolve().parents[3]
PROFILE: StrategyProfile = load_profile(REPO / "strategies" / "fast_momentum_v1.yaml")
BTC = "BTC/USD"
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
RULES = AssetRules(
    min_qty=D("0.0001"), qty_increment=D("0.0001"), min_notional=D("1"), price_increment=D("0.01")
)
FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"))


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

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return list(self.bars) if symbol == BTC and timeframe == "5m" else []

    def quote(self, symbol: str) -> Quote | None:
        return self.q if symbol == BTC else None

    def last_price(self, symbol: str) -> float | None:
        return self.last if symbol == BTC else None

    def is_stale(self, symbol: str) -> bool:
        return self.stale

    def spread_bps(self, symbol: str) -> float | None:
        return None if self.q is None else (self.q.ask - self.q.bid) / self.q.ask * 10_000


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
) -> RiskEngine:
    return RiskEngine(
        broker,
        profile=PROFILE,
        strategy_id=1,
        mode="paper",
        gate=gate,
        market=view or FakeView(),
        sessions=sessions,
        trading_day_tz=tz,
        clock=clock or Clock(NOW),
        io_timeout_s=io_timeout_s,
    )


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


async def test_approved_entry_is_persisted_and_submitted(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    result = await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert result.decision.approved and result.status == "new"
    (row,) = await orders(sessions)
    assert (row.client_order_id, row.status, row.broker_order_id) == (
        f"at-c{cycle_id}-e",
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


async def test_first_check_snapshots_equity_as_baseline(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    await make_engine_(sim, sessions, gate).check(intent(cycle_id))
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

    async def get_account(self) -> Account:
        account = await super().get_account()
        if self.equity is None:
            return account
        return account.model_copy(update={"equity": self.equity})


async def snapshots(sessions: async_sessionmaker[AsyncSession]) -> list[m.EquitySnapshot]:
    async with sessions() as s:
        return list(await s.scalars(select(m.EquitySnapshot).order_by(m.EquitySnapshot.ts)))


@pytest.mark.parametrize("restart", [False, True])
async def test_equity_peak_seen_by_a_rejected_check_still_bounds_drawdown(
    restart: bool, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = _sim(EquityBroker)
    assert isinstance(broker, EquityBroker)
    clock = Clock(NOW)
    engine = make_engine_(broker, sessions, gate, clock=clock)
    await engine.snapshot_equity()  # baseline and HWM 1000
    broker.equity = D("1200")  # spike between two snapshots
    clock.now = NOW + timedelta(seconds=10)
    spiked = await engine.check(intent(cycle_id, stop=D("100.10")))  # invalid stop
    assert "not below entry" in spiked.decision.reasons[0]
    assert [s.hwm for s in await snapshots(sessions)] == [D("1000"), D("1200")]
    broker.equity = D("1000")  # back down before the next periodic snapshot
    clock.now = NOW + timedelta(seconds=20)
    if restart:
        engine = make_engine_(broker, sessions, gate, clock=clock)
    result = await engine.check(intent(await new_cycle(sessions)))
    assert not result.decision.approved and "drawdown" in result.decision.reasons[0]
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_KEY) is True
    assert await orders(sessions) == []


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
    assert (row.side, row.type, row.qty, row.client_order_id) == ("sell", "market", D("1"), None)
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

    async def get_account(self) -> Account:
        if self.entered is not None:
            self.entered.set()
        await hang()


async def test_hung_entry_gather_cannot_block_an_exit(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = _sim(HangingAccount)
    assert isinstance(broker, HangingAccount)
    broker.entered = asyncio.Event()
    await _held_btc(broker)
    engine = make_engine_(broker, sessions, gate, io_timeout_s=0.05)
    entry = asyncio.create_task(engine.check(intent(cycle_id)))
    try:
        await broker.entered.wait()  # the entry holds the lock inside the hung call
        exited = await asyncio.wait_for(engine.exit(ExitRequest(symbol=BTC, reason="stop")), 2)
        assert exited.decision.approved and exited.status == "new"
        checked = await asyncio.wait_for(entry, 2)
    finally:
        entry.cancel()
        await asyncio.gather(entry, return_exceptions=True)
    assert checked.decision.reasons == ("state unavailable: TimeoutError",)
    assert [(d.kind, d.approved) for d in await decisions(sessions)] == [
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
    engine = make_engine_(_sim(HangingSubmit), sessions, gate, io_timeout_s=0.05)
    result = await asyncio.wait_for(engine.check(intent(cycle_id)), 2)
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
    engine = make_engine_(sim, sessions, StuckGate(fail), io_timeout_s=0.05)
    result = await asyncio.wait_for(engine.check(intent(cycle_id)), 2)
    assert result.status == "not_submitted"
    (row,) = await orders(sessions)
    assert (row.status, row.broker_order_id) == ("not_submitted", None)


class HangingPositions(SimBroker):
    async def get_positions(self) -> list[Position]:
        await hang()


class HangingClose(SimBroker):
    async def close_position(self, symbol: str) -> OrderAck:
        await hang()


async def test_hung_exit_state_rejects_and_hung_close_is_unknown(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    request = ExitRequest(symbol=BTC, reason="stop")
    stuck = make_engine_(_sim(HangingPositions), sessions, gate, io_timeout_s=0.05)
    rejected = await asyncio.wait_for(stuck.exit(request), 2)
    assert rejected.decision.reasons == ("state unavailable: TimeoutError",)
    assert await orders(sessions) == []

    broker = _sim(HangingClose)
    await _held_btc(broker)
    closing = make_engine_(broker, sessions, gate, io_timeout_s=0.05)
    result = await asyncio.wait_for(closing.exit(request), 2)
    assert result.decision.approved and result.status == "unknown"
    (row,) = await orders(sessions)
    assert (row.side, row.status, row.broker_order_id) == ("sell", "unknown", None)
