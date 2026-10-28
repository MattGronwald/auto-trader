"""Position Manager kill handling, external sales and flatten (WP 1.5c, spec 2026-10-19
sections 4.4 kill handling, 4.4.1 and 4.7) on SimBroker, the real RiskEngine and Ledger and a
migrated SQLite journal."""

from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping
from contextlib import asynccontextmanager
from datetime import timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.broker.base import BrokerOrder, OrderAck, OrderRejected
from autotrader.broker.sim import FillPlan, SimBroker
from autotrader.core.bus import EventBus
from autotrader.core.flatten import (
    FlattenRequest,
    cancel_flatten,
    read_request,
    read_result,
    request_flatten,
)
from autotrader.core.kill import PAUSED_KEY, ControlStatus
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import OrderResult, RiskEngine
from autotrader.risk.types import CancelResult, ExitRequest, HeldPosition, RiskDecision
from tests.dbhooks import CommitHook, hooked_sessionmaker
from tests.pm_harness import BTC, ETH, T0, Harness, app_config, event_payloads, make_sim

Logs = list[MutableMapping[str, Any]]
KILLED = ControlStatus(True, ("kill set",))
CLEAR = ControlStatus(False, ())


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
def other_db_url(tmp_path: Path) -> str:
    """A second, independent journal inside one test (migrated outside the event loop)."""
    url = f"sqlite+aiosqlite:///{tmp_path / 'other.db'}"
    upgrade(url)
    return url


@asynccontextmanager
async def fresh_journal(
    url: str,
) -> AsyncIterator[tuple[async_sessionmaker[AsyncSession], EventBus]]:
    engine = make_engine(url)
    sessions = make_sessionmaker(engine)
    bus = EventBus(sessions)
    try:
        yield sessions, bus
    finally:
        await bus.close()
        await engine.dispose()


def named(logs: Logs, event: str) -> Logs:
    return [line for line in logs if line["event"] == event]


# --- test doubles ----------------------------------------------------------------------


class CancelSpy(SimBroker):
    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self.cancels: list[str] = []
        self.fail: set[str] = set()

    async def cancel_order(self, order_id: str) -> None:
        if order_id in self.fail:
            raise OrderRejected("cannot cancel")
        self.cancels.append(order_id)
        await super().cancel_order(order_id)


class CannotTrade(SimBroker):
    def can_place_orders(self) -> bool:
        return False


class EthCloseRejected(SimBroker):
    async def close_position(self, symbol: str) -> OrderAck:
        if symbol == ETH:
            raise OrderRejected(f"no {symbol} today")
        return await super().close_position(symbol)


class EthAckLost(SimBroker):
    """The ETH close lands at the broker, but its acknowledgement is lost."""

    async def close_position(self, symbol: str) -> OrderAck:
        ack = await super().close_position(symbol)
        if symbol == ETH:
            raise RuntimeError("connection reset")
        return ack


class BtcAckLost(SimBroker):
    """The BTC close raises before reaching the broker: the engine records `unknown`."""

    async def close_position(self, symbol: str) -> OrderAck:
        if symbol == BTC:
            raise RuntimeError("connection reset")
        return await super().close_position(symbol)


class RacingEngine(RiskEngine):
    """Awaits `callback` once, right after the read, on the `fire_on_call`-th holdings read."""

    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self.callback: Callable[[], Awaitable[None]] | None = None
        self.fire_on_call = 0
        self.calls = 0

    async def sellable_positions(self) -> tuple[HeldPosition, ...]:
        held = await super().sellable_positions()
        self.calls += 1
        if self.callback is not None and self.calls == self.fire_on_call:
            callback, self.callback = self.callback, None
            await callback()
        return held


class ExternalExitRaises(RiskEngine):
    async def exit(self, request: ExitRequest) -> OrderResult:
        if request.external and request.symbol == ETH:
            raise RuntimeError("journal down")
        return await super().exit(request)


class HoldingsDown(RiskEngine):
    async def sellable_positions(self) -> tuple[HeldPosition, ...]:
        raise RuntimeError("broker down")


class CancelDown(RiskEngine):
    async def cancel_open_entries(self, exclude: Any = ()) -> CancelResult:
        raise RuntimeError("broker down")


class ScriptedExternal(RiskEngine):
    """External exits return `outcome` without touching the broker or the journal."""

    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self.external_calls = 0
        self.outcome = OrderResult(RiskDecision(approved=True, reasons=()), status="unknown")

    async def exit(self, request: ExitRequest) -> OrderResult:
        if request.external:
            self.external_calls += 1
            return self.outcome
        return await super().exit(request)


class BrokenControls:
    @property
    def latest(self) -> ControlStatus | None:
        raise RuntimeError("control source down")


# --- helpers ---------------------------------------------------------------------------


async def hold_externally(h: Harness, symbol: str = ETH, qty: str = "1") -> None:
    """A holding the journal doesn't know: bought at the sim with no order row."""
    await h.sim.submit_order(BrokerOrder(symbol=symbol, side="buy", qty=D(qty), type="market"))
    h.bar(symbol, 100.0)


async def ask_flatten(h: Harness) -> FlattenRequest:
    request, created = await request_flatten(h.sessions, source="cli", reason="t", now=h.clock.now)
    assert created
    return request


async def result_of(h: Harness) -> dict[str, Any] | None:
    async with h.sessions() as s:
        return await read_result(s)


async def pending_request(h: Harness) -> FlattenRequest | None:
    async with h.sessions() as s:
        return await read_request(s)


async def all_orders(h: Harness) -> list[m.Order]:
    async with h.sessions() as s:
        return list(await s.scalars(select(m.Order).order_by(m.Order.id)))


def racing(h: Harness) -> RacingEngine:
    assert isinstance(h.engine, RacingEngine)
    return h.engine


# --- kill handling ---------------------------------------------------------------------


async def test_a_kill_cancels_open_entries_once_and_never_a_sell(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    sim = make_sim(cls=CancelSpy)
    assert isinstance(sim, CancelSpy)
    h = Harness(sessions, bus, sim=sim)
    await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()  # a stop exit in flight (sell, status new)
    _, entry_id = await h.submit_entry(ETH)  # a resting limit buy at the broker
    h.controls.latest = KILLED
    await h.pm.step()
    await h.pm.step()
    (sell,) = await h.sells()
    assert sim.cancels == [entry_id]  # once, never the sell
    assert sell.broker_order_id is not None
    assert (await sim.get_order(sell.broker_order_id)).status == "new"
    assert (await sim.get_order(entry_id)).status == "canceled"


async def test_a_kill_already_set_at_startup_cancels_on_the_first_step(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    _, entry_id = await h.submit_entry(ETH)
    h.controls.latest = KILLED
    await h.new_pm().step()
    assert (await h.sim.get_order(entry_id)).status == "canceled"


async def test_kill_flatten_exits_every_position_with_reason_kill(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, config=app_config(kill_flatten=True))
    btc = await h.open_position(BTC)
    eth = await h.open_position(ETH)
    h.quote(BTC, 100.0)
    h.controls.latest = KILLED
    await h.pm.step()
    assert [(await h.position(p)).exit_trigger for p in (btc, eth)] == ["kill", "kill"]
    h.bar(BTC, 100.0)
    h.bar(ETH, 100.0)
    await h.sync()
    for pid in (btc, eth):
        pos = await h.position(pid)
        assert (pos.closed_at is not None, pos.exit_reason) == (True, "kill")


async def test_kill_flatten_exits_a_late_entry_fill(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, config=app_config(kill_flatten=True))
    h.controls.latest = KILLED
    row_id, broker_id = await h.submit_entry(BTC, limit=None, ack=False)
    with capture_logs() as logs:
        await h.pm.step()
        await h.pm.step()
    (unresolved,) = named(logs, "positions.entry_unresolved")
    assert unresolved["order_row"] == row_id
    async with sessions() as s:
        await repo.update_order(s, row_id, status="new", now=h.clock.now, broker_order_id=broker_id)
    h.bar(BTC, 100.0)
    await h.sync()
    (entry,) = [o for o in await all_orders(h) if o.id == row_id]
    assert entry.position_id is not None
    await h.pm.step()
    assert (await h.position(entry.position_id)).exit_trigger == "kill"
    (sell,) = await h.sells()
    assert (sell.exit_reason, sell.external) == ("kill", False)


async def test_without_kill_flatten_positions_keep_normal_management(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)  # paper: kill.flatten defaults to false
    pid = await h.open_position()
    h.controls.latest = KILLED
    h.quote(BTC, 100.0)
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger is None
    h.quote(BTC, 95.0)
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "stop"


async def test_clearing_the_kill_stops_cancels_and_keeps_latched_exits(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    sim = make_sim(cls=CancelSpy)
    assert isinstance(sim, CancelSpy)
    h = Harness(sessions, bus, sim=sim, config=app_config(kill_flatten=True))
    pid = await h.open_position()
    _, entry_id = await h.submit_entry(ETH)
    sim.fail.add(entry_id)
    h.controls.latest = KILLED
    with capture_logs() as logs:
        await h.pm.step()
    assert len(named(logs, "risk.cancel_failed")) == 1
    assert (await h.position(pid)).exit_trigger == "kill"
    sim.fail.clear()
    h.controls.latest = CLEAR
    h.clock.advance(1)
    await h.pm.step()
    assert sim.cancels == []  # no further cancel after the clear
    assert (await h.sim.get_order(entry_id)).status == "new"
    assert len(await h.sells()) == 1
    h.bar(BTC, 100.0)
    await h.sync()
    pos = await h.position(pid)
    assert (pos.closed_at is not None, pos.exit_reason) == (True, "kill")


# --- external holdings -----------------------------------------------------------------


async def test_flatten_sells_positions_and_external_holdings_then_reports_flat(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position(BTC)
    await hold_externally(h, ETH)
    async with sessions() as s:
        s.add(m.SymbolDust(symbol=BTC, qty=D("0.00005"), updated_at=T0))
        await s.commit()
    request = await ask_flatten(h)
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "flatten"
    (eth_sell,) = await h.sells(ETH)
    assert (eth_sell.external, eth_sell.exit_reason) == (True, "flatten")
    h.bar(BTC, 100.0)
    h.bar(ETH, 100.0)
    await h.sync()
    with capture_logs() as logs:
        await h.pm.step()
    result = await result_of(h)
    assert result is not None
    assert (result["request_ts"], result["flat"]) == (request.ts, True)
    assert await pending_request(h) is None
    assert result["dust"] == {BTC: "0.00005"}
    (sale,) = result["external_sales"]
    assert (sale["symbol"], sale["external_qty"], sale["order_row_id"]) == (ETH, "1", eth_sell.id)
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_KEY) is True
    (completed,) = await event_payloads(sessions, "FlattenCompleted")
    assert (completed["request_ts"], completed["dust"]) == (request.ts, {BTC: "0.00005"})
    assert await event_payloads(sessions, "LedgerAnomaly") == []
    (flat,) = named(logs, "positions.flat")
    assert flat["dust"] == {BTC: "0.00005"}


async def test_kill_flatten_sells_an_external_holding(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, config=app_config(kill_flatten=True))
    await hold_externally(h, ETH)
    h.controls.latest = KILLED
    await h.pm.step()
    (sell,) = await h.sells(ETH)
    assert (sell.external, sell.exit_reason) == (True, "kill")
    h.bar(ETH, 100.0)
    await h.sync()
    h.clock.advance(1)
    await h.pm.step()
    assert len(await h.sells(ETH)) == 1
    assert len(await event_payloads(sessions, "ExternalHoldingSold")) == 1


async def test_an_external_holding_waits_for_an_unsettled_entry_on_its_symbol(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    await hold_externally(h, ETH)
    row_id, _ = await h.submit_entry(ETH, ack=False)  # id-less pending buy
    await ask_flatten(h)
    await h.pm.step()
    assert await h.sells(ETH) == []
    result = await result_of(h)
    assert result is not None and result["flat"] is False
    (blocking,) = result["remaining"]["orders"]
    assert (blocking["order_row_id"], blocking["side"], blocking["unresolved"]) == (
        row_id,
        "buy",
        True,
    )
    assert result["remaining"]["holdings"] == [{"symbol": ETH, "qty": "1"}]
    async with sessions() as s:
        await repo.update_order(s, row_id, status="not_submitted", now=h.clock.now)
    await h.sync()
    h.clock.advance(1)
    await h.pm.step()
    (sell,) = await h.sells(ETH)
    assert sell.external is True
    h.bar(ETH, 100.0)
    await h.sync()
    assert [p["kind"] for p in await event_payloads(sessions, "LedgerAnomaly")] == []


async def test_external_failures_back_off_per_symbol_and_alert_after_three(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, other_db_url: str
) -> None:
    async def run(
        sessions: async_sessionmaker[AsyncSession], bus: EventBus, *, sync: bool
    ) -> tuple[int, Logs]:
        h = Harness(sessions, bus, sim=make_sim(cls=EthCloseRejected))
        await hold_externally(h, ETH)
        await ask_flatten(h)
        with capture_logs() as logs:
            for t in (0, 1, 3):
                h.clock.now = T0 + timedelta(seconds=t)
                if sync:
                    await h.sync()  # settles the id-less rejected row (clarification 15)
                await h.pm.step()
        return len(await h.sells(ETH)), logs

    rows, logs = await run(sessions, bus, sync=True)
    assert rows == 3
    (failing,) = named(logs, "positions.exit_failing")
    assert (failing["external"], failing["symbol"], failing["failures"]) == (True, ETH, 3)
    async with fresh_journal(other_db_url) as (other, other_bus):
        rows, logs = await run(other, other_bus, sync=False)
    assert rows == 1  # the unsettled rejected row blocks the symbol
    assert named(logs, "positions.exit_failing") == []


async def test_a_failing_external_sale_never_stops_position_exits(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, engine_cls=ExternalExitRaises)
    pid = await h.open_position(BTC)
    await hold_externally(h, ETH)
    await ask_flatten(h)
    with capture_logs() as logs:
        await h.pm.step()
    (failed,) = named(logs, "positions.external_failed")
    assert failed["symbol"] == ETH
    assert (await h.position(pid)).exit_trigger == "flatten"
    assert len(await h.sells(BTC)) == 1


async def test_accepted_then_rejected_external_sales_back_off_and_alert_after_three(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    for _ in range(3):
        h.sim.script(ETH, "sell", FillPlan(("reject",)))
    await hold_externally(h, ETH)
    await ask_flatten(h)
    created: list[float] = []
    elapsed = 0.0

    async def step_at(t: float) -> None:
        nonlocal elapsed
        h.clock.advance(t - elapsed)
        elapsed = t
        before = len(await h.sells(ETH))
        await h.pm.step()
        if len(await h.sells(ETH)) > before:
            created.append(t)

    with capture_logs() as logs:
        for boundary, next_boundary in ((0.0, 1.0), (1.0, 3.0), (3.0, None)):
            await step_at(boundary)  # an external sell, acknowledged "new"
            assert (await h.sells(ETH))[-1].status == "new"
            h.bar(ETH, 100.0)
            await h.sync()  # rejected and settled (zero fill: no ExternalHoldingSold)
            await step_at(boundary)  # judges the settled row: one failure
            if next_boundary is not None:
                await step_at(next_boundary - 0.5)
        await h.sync()
        await step_at(elapsed)
        await step_at(elapsed + 0.5)
    assert created == [0.0, 1.0, 3.0]
    (failing,) = named(logs, "positions.exit_failing")
    assert (failing["external"], failing["symbol"], failing["failures"]) == (True, ETH, 3)
    assert failing["order_row"] == (await h.sells(ETH))[-1].id


async def test_an_unknown_external_exit_alerts_every_60_s_without_a_holding(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=EthAckLost), config=app_config(kill_flatten=True))
    await hold_externally(h, ETH)
    h.controls.latest = KILLED
    with capture_logs() as logs:
        await h.pm.step()  # the sell lands at the broker; the engine records `unknown`
        h.bar(ETH, 100.0)  # and fills it: the broker holds nothing now
        for t in (1, 30, 61):
            h.clock.now = T0 + timedelta(seconds=t)
            await h.pm.step()
    (sell,) = await h.sells(ETH)
    assert (sell.status, sell.broker_order_id, sell.external) == ("unknown", None, True)
    lines = named(logs, "positions.exit_unresolved")
    assert len(lines) == 2  # at 0 and at 61 s
    assert all(line["order_rows"] == [sell.id] and line["external"] for line in lines)
    h.controls.latest = CLEAR
    with capture_logs() as logs:
        for t in (121, 150, 181):
            h.clock.now = T0 + timedelta(seconds=t)
            await h.pm.step()
    assert len(named(logs, "positions.exit_unresolved")) == 2
    assert len(await h.sells(ETH)) == 1


async def test_a_stuck_external_sell_alerts_without_a_second_sell(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, config=app_config(kill_flatten=True))
    await hold_externally(h, ETH)
    h.controls.latest = KILLED
    await h.pm.step()  # an external sell, `new` with a broker id; no ETH bars follow
    with capture_logs() as logs:
        for t in (30, 61, 121):
            h.clock.now = T0 + timedelta(seconds=t)
            await h.pm.step()
    (sell,) = await h.sells(ETH)
    lines = named(logs, "positions.exit_stuck")
    assert len(lines) == 2
    assert all(line["order_rows"] == [sell.id] and line["external"] for line in lines)


# --- the flat check --------------------------------------------------------------------


async def test_flat_is_not_reported_while_an_entry_is_unsettled_or_an_exit_unresolved(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=BtcAckLost))
    await h.open_position(BTC)
    entry_row, entry_id = await h.submit_entry(ETH)  # a resting limit buy
    await ask_flatten(h)
    await h.pm.step()  # cancel requested (not yet synced); BTC exit `unknown`, id-less
    assert (await h.sim.get_order(entry_id)).status == "canceled"
    result = await result_of(h)
    assert result is not None and result["flat"] is False
    (btc_sell,) = await h.sells(BTC)
    orders = {o["order_row_id"]: o for o in result["remaining"]["orders"]}
    assert (orders[entry_row]["status"], orders[entry_row]["unresolved"]) == ("new", False)
    assert (orders[btc_sell.id]["status"], orders[btc_sell.id]["unresolved"]) == (
        "unknown",
        True,
    )
    await h.sync()  # the entry's cancel is booked and settles
    h.clock.advance(10)
    await h.pm.step()
    result = await result_of(h)
    assert result is not None and result["flat"] is False
    (only,) = result["remaining"]["orders"]
    assert (only["order_row_id"], only["side"], only["unresolved"]) == (btc_sell.id, "sell", True)
    assert await pending_request(h) is not None


async def test_a_fill_landing_between_the_flat_check_reads_delays_completion(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, engine_cls=RacingEngine)
    row_id, broker_id = await h.submit_entry(ETH, limit=None, ack=False)
    await ask_flatten(h)

    async def ack_fill_and_book() -> None:
        async with sessions() as s:
            await repo.update_order(
                s, row_id, status="new", now=h.clock.now, broker_order_id=broker_id
            )
        h.bar(ETH, 100.0)
        await h.sync()

    racing(h).callback = ack_fill_and_book
    racing(h).fire_on_call = 2  # the flat check's holdings read
    await h.pm.step()
    assert await pending_request(h) is not None
    (entry,) = await all_orders(h)
    assert entry.position_id is not None and entry.settled_at is not None
    h.clock.advance(1)
    await h.pm.step()
    assert (await h.position(entry.position_id)).exit_trigger == "flatten"
    (sell,) = await h.sells(ETH)
    assert sell.external is False
    assert await pending_request(h) is not None
    h.bar(ETH, 100.0)
    await h.sync()
    h.clock.advance(1)
    await h.pm.step()
    result = await result_of(h)
    assert result is not None and result["flat"] is True
    assert result["external_sales"] == []


async def test_a_changed_generation_blocks_a_completion_that_looks_flat(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, engine_cls=RacingEngine)
    await ask_flatten(h)

    async def settle_a_local_order() -> None:
        async with sessions() as s:
            row = await repo.insert_order(
                s,
                cycle_id=None,
                client_order_id=None,
                symbol=BTC,
                mode="paper",
                type_="market",
                side="buy",
                qty=D(1),
                limit_price=None,
                stop_price=None,
                tp_price=None,
                status="not_submitted",
                now=h.clock.now,
            )
            await s.execute(update(m.Order).where(m.Order.id == row.id).values(settled_at=T0))
            await s.commit()

    racing(h).callback = settle_a_local_order
    racing(h).fire_on_call = 2
    await h.pm.step()
    assert await pending_request(h) is not None
    assert await event_payloads(sessions, "FlattenCompleted") == []
    h.clock.advance(1)
    await h.pm.step()
    assert await pending_request(h) is None
    assert len(await event_payloads(sessions, "FlattenCompleted")) == 1


async def test_a_completion_never_overwrites_a_cancelled_request(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, engine_cls=RacingEngine)
    request = await ask_flatten(h)

    async def cancel() -> None:
        await cancel_flatten(sessions, now=h.clock.now)

    racing(h).callback = cancel
    racing(h).fire_on_call = 2
    await h.pm.step()
    result = await result_of(h)
    assert result is not None
    assert (result["request_ts"], result["cancelled"], result["flat"]) == (request.ts, True, False)
    assert await event_payloads(sessions, "FlattenCompleted") == []


async def test_a_pending_refresh_never_overwrites_a_cancel_record(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=CannotTrade), engine_cls=RacingEngine)
    await h.open_position(BTC)
    await ask_flatten(h)

    async def cancel() -> None:
        await cancel_flatten(sessions, now=h.clock.now)

    racing(h).callback = cancel
    racing(h).fire_on_call = 1  # cannot trade: the flat check is the only read
    await h.pm.step()
    result = await result_of(h)
    assert result is not None and result["cancelled"] is True
    assert "remaining" not in result


async def test_a_broker_that_cannot_trade_keeps_flatten_pending_with_a_reason(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=CannotTrade))
    pid = await h.open_position(BTC)
    request = await ask_flatten(h)
    with capture_logs() as logs:
        await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "flatten"
    assert await h.sells(BTC) == []
    assert await h.sells(ETH) == []
    assert await result_of(h) == {
        "request_ts": request.ts,
        "flat": False,
        "remaining": {
            "positions": [{"position_id": pid, "symbol": BTC, "qty": "1"}],
            "holdings": [{"symbol": BTC, "qty": "1"}],
            "orders": [],
        },
        "reason": "broker cannot place orders",
        "ts": T0.isoformat(),
    }
    (pending,) = named(logs, "positions.flatten_pending")
    assert pending["request_ts"] == request.ts
    with capture_logs() as logs:
        h.clock.advance(5)
        await h.pm.step()
    assert named(logs, "positions.flatten_pending") == []
    with capture_logs() as logs:
        h.clock.advance(5)
        await h.pm.step()
    assert len(named(logs, "positions.flatten_pending")) == 1
    result = await result_of(h)
    assert result is not None and result["ts"] == (T0 + timedelta(seconds=10)).isoformat()


# --- paths the dry run left unreached ---------------------------------------------------


async def test_an_uncertain_completion_commit_is_retried_next_step(db_url: str) -> None:
    engine = make_engine(db_url)
    hook = CommitHook()
    sessions = hooked_sessionmaker(engine, hook)
    bus = EventBus(sessions)
    try:
        h = Harness(sessions, bus)
        await ask_flatten(h)
        hook.fail_before = 1
        with capture_logs() as logs:
            await h.pm.step()
        assert len(named(logs, "positions.flatten_uncertain")) == 1
        assert named(logs, "positions.flat") == []
        assert await pending_request(h) is not None
        h.clock.advance(1)
        await h.pm.step()
        assert await pending_request(h) is None
        assert len(await event_payloads(sessions, "FlattenCompleted")) == 1
    finally:
        await bus.close()
        await engine.dispose()


async def test_a_failed_holdings_read_reports_holdings_unavailable(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, engine_cls=HoldingsDown)
    await ask_flatten(h)
    with capture_logs() as logs:
        await h.pm.step()
    assert len(named(logs, "positions.flatten_read_failed")) == 2  # external sales, flat check
    result = await result_of(h)
    assert result is not None and result["flat"] is False
    assert result["remaining"]["holdings"] == "unavailable"


async def test_a_failed_cancel_never_stops_the_position_loop(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, engine_cls=CancelDown)
    pid = await h.open_position(BTC)
    h.controls.latest = KILLED
    h.quote(BTC, 95.0)
    with capture_logs() as logs:
        await h.pm.step()
    assert len(named(logs, "positions.control_failed")) == 1
    assert (await h.position(pid)).exit_trigger == "stop"
    assert len(await h.sells(BTC)) == 1


_REJECTED = RiskDecision(approved=False, reasons=("x",))


@pytest.mark.parametrize(
    "outcome",
    [
        OrderResult(RiskDecision(approved=True, reasons=()), status="unknown"),
        OrderResult(_REJECTED, no_position=True),
        OrderResult(_REJECTED, dust=True, min_qty=D("0.0001")),
    ],
    ids=["unknown", "no_position", "dust"],
)
async def test_an_unknown_dust_or_vanished_external_exit_counts_no_failure(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, outcome: OrderResult
) -> None:
    h = Harness(sessions, bus, engine_cls=ScriptedExternal)
    assert isinstance(h.engine, ScriptedExternal)
    h.engine.outcome = outcome
    await hold_externally(h, ETH)
    await ask_flatten(h)
    with capture_logs() as logs:
        await h.pm.step()
        await h.pm.step()  # same instant: a counted failure would make the backoff not due
    assert h.engine.external_calls == 2
    assert named(logs, "positions.exit_failing") == []


async def test_a_failing_control_source_never_stops_the_position_loop(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position(BTC)
    h.controls = BrokenControls()  # type: ignore[assignment]
    pm = h.new_pm()
    h.quote(BTC, 95.0)
    with capture_logs() as logs:
        await pm.step()
    assert len(named(logs, "positions.control_failed")) == 1
    assert (await h.position(pid)).exit_trigger == "stop"
    assert len(await h.sells(BTC)) == 1
