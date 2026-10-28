"""Position Manager (WP 1.5c, spec 2026-10-19 sections 4.3-4.4) on SimBroker, the real
RiskEngine and Ledger and a migrated SQLite journal.

Prices: entry 100, stop 95, TP 110, ATR 2, R 5, trailing activation 105, trail distance 3,
quote fees 25 bps."""

from collections.abc import AsyncIterator, MutableMapping
from datetime import timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.broker.base import (
    DustRemainder,
    OrderAck,
    OrderMethodsUnavailable,
    OrderRejected,
)
from autotrader.broker.sim import FillPlan, SimBroker
from autotrader.core.bus import EventBus
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import OrderResult, RiskEngine
from autotrader.risk.types import ExitRequest
from tests.dbhooks import CommitHook, hooked_sessionmaker
from tests.pm_harness import (
    BASE_FEES,
    BTC,
    ETH,
    PROFILE,
    RULES,
    T0,
    Harness,
    app_config,
    event_payloads,
    make_sim,
)

Logs = list[MutableMapping[str, Any]]


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


def named(logs: Logs, event: str) -> Logs:
    return [line for line in logs if line["event"] == event]


async def exit_decisions(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as s:
        n = await s.scalar(
            select(func.count()).select_from(m.RiskDecision).where(m.RiskDecision.kind == "exit")
        )
    return int(n or 0)


class RejectingClose(SimBroker):
    async def close_position(self, symbol: str) -> OrderAck:
        raise OrderRejected(f"no {symbol} today")


class UnavailableClose(SimBroker):
    async def close_position(self, symbol: str) -> OrderAck:
        raise OrderMethodsUnavailable("stub")


class LostClose(SimBroker):
    async def close_position(self, symbol: str) -> OrderAck:
        raise RuntimeError("connection reset")


class DustClose(SimBroker):
    async def close_position(self, symbol: str) -> OrderAck:
        raise DustRemainder(BTC, D("0.00005"), RULES.min_qty)


class CannotTrade(SimBroker):
    def can_place_orders(self) -> bool:
        return False


class BtcExitRaises(RiskEngine):
    async def exit(self, request: ExitRequest) -> OrderResult:
        if request.symbol == BTC:
            raise RuntimeError("journal down")
        return await super().exit(request)


# --- triggers --------------------------------------------------------------------------


async def test_a_stop_exit_end_to_end(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    pos = await h.position(pid)
    assert (pos.exit_trigger, pos.exit_triggered_at) == ("stop", T0)
    assert pos.time_limit_at == pos.opened_at + timedelta(minutes=PROFILE.risk.max_hold_min)
    h.bar(BTC, 95.0)
    await h.sync()
    pos = await h.position(pid)
    assert (pos.closed_at is not None, pos.exit_reason, pos.realized_pnl) == (
        True,
        "stop",
        D("-5.4875"),  # -100.25 + 95 - 0.2375
    )
    (triggered,) = await event_payloads(sessions, "ExitTriggered")
    assert (triggered["reason"], D(triggered["bid"])) == ("stop", D("95"))


async def test_a_take_profit_exit(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position()
    h.quote(BTC, 110.0)
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "take_profit"
    h.bar(BTC, 110.0)
    await h.sync()
    pos = await h.position(pid)
    assert (pos.closed_at is not None, pos.exit_reason) == (True, "take_profit")


async def test_a_time_exit_needs_no_price(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position()
    await h.pm.step()
    pos = await h.position(pid)
    assert pos.time_limit_at is not None
    assert pos.exit_trigger is None
    h.clock.now = pos.time_limit_at - timedelta(seconds=1)
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger is None
    h.clock.now = pos.time_limit_at
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "time"


async def test_a_trailing_exit_after_activation_and_pullback(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position()
    h.quote(BTC, 106.0)
    await h.pm.step()
    pos = await h.position(pid)
    assert (pos.trail_high, pos.trail_stop, pos.exit_trigger) == (D("106"), D("103"), None)
    h.quote(BTC, 104.0)
    await h.pm.step()
    pos = await h.position(pid)
    assert (pos.trail_high, pos.trail_stop, pos.exit_trigger) == (D("106"), D("103"), None)
    h.quote(BTC, 103.0)
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "trailing"


async def test_a_stale_exit_only_when_enabled(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position()
    h.view.ages[BTC] = 301.0
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger is None
    h.config = app_config(stale_exit=True)
    h.pm = h.new_pm()
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "stale"


async def test_a_never_priced_symbol_gets_the_cold_start_grace(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, config=app_config(stale_exit=True))
    pid = await h.open_position()
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger is None
    h.clock.advance(301)
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "stale"


async def test_an_old_quote_on_a_fresh_symbol_triggers_nothing(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position()
    h.quote(BTC, 95.0, age_s=61)  # stale_alert_s is 60; the feed says the symbol is fresh
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger is None
    h.quote(BTC, 95.0, age_s=60)
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "stop"


# --- the exit path ---------------------------------------------------------------------


async def test_a_partial_entry_is_protected_and_a_late_fill_is_exited(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    h.sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    pid = await h.open_position()
    assert (await h.position(pid)).qty == D("0.5")
    h.quote(BTC, 95.0)
    await h.pm.step()
    assert [s.qty for s in await h.sells()] == [D("0.5")]
    h.bar(BTC, 95.0)  # the entry remainder and the sell both fill
    await h.sync()
    pos = await h.position(pid)
    assert (pos.qty, pos.closed_at) == (D("0.5"), None)
    await h.pm.step()
    assert len(await h.sells()) == 2
    h.bar(BTC, 95.0)
    await h.sync()
    pos = await h.position(pid)
    assert (pos.closed_at is not None, pos.exit_reason) == (True, "stop")
    assert len(await h.sells()) == 2


async def test_a_latched_stop_still_exits_after_the_price_recovers(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    h.sim.script(BTC, "sell", FillPlan(("reject",)))
    pid = await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    h.bar(BTC, 95.0)
    await h.sync()  # the sell is rejected and settled
    h.quote(BTC, 100.0)
    await h.pm.step()
    assert len(await h.sells()) == 1  # the settled zero fill counted one failure: 1 s
    h.clock.advance(1)
    await h.pm.step()
    assert len(await h.sells()) == 2
    h.bar(BTC, 100.0)
    await h.sync()
    pos = await h.position(pid)
    assert (pos.closed_at is not None, pos.exit_reason) == (True, "stop")


async def test_accepted_then_rejected_exits_back_off_and_alert_after_three(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    for _ in range(3):
        h.sim.script(BTC, "sell", FillPlan(("reject",)))
    await h.open_position()
    h.quote(BTC, 95.0)
    created: list[float] = []
    elapsed = 0.0

    async def step_at(t: float) -> None:
        nonlocal elapsed
        h.clock.advance(t - elapsed)
        elapsed = t
        before = len(await h.sells())
        await h.pm.step()
        if len(await h.sells()) > before:
            created.append(t)

    with capture_logs() as logs:
        for boundary, next_boundary in ((0.0, 1.0), (1.0, 3.0), (3.0, None)):
            await step_at(boundary)  # a sell, acknowledged "new"
            assert (await h.sells())[-1].status == "new"
            h.bar(BTC, 95.0)
            await h.sync()  # rejected and settled
            await step_at(boundary)  # judges the settled row: one failure
            if next_boundary is not None:
                await step_at(next_boundary - 0.5)
        await h.sync()
        await step_at(elapsed)
        await step_at(elapsed + 0.5)
    assert created == [0.0, 1.0, 3.0]
    (failing,) = named(logs, "positions.exit_failing")
    assert (failing["failures"], failing["status"]) == (3, "rejected")
    assert failing["order_row"] == (await h.sells())[-1].id
    assert len(await h.sells()) == 3


async def test_a_filled_exit_resets_the_failure_count(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    h.sim.script(BTC, "sell", FillPlan(("reject",)))
    h.sim.script(BTC, "sell", FillPlan((D("0.5"), "cancel")))
    pid = await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    h.bar(BTC, 95.0)
    await h.sync()  # accepted, then rejected
    await h.pm.step()  # one failure: the next attempt waits 1 s
    assert len(await h.sells()) == 1
    h.clock.advance(1)
    await h.pm.step()
    assert len(await h.sells()) == 2
    h.bar(BTC, 95.0)  # fills 0.5
    h.bar(BTC, 95.0)  # cancels the rest
    await h.sync()
    assert (await h.position(pid)).qty == D("0.5")
    await h.pm.step()  # real progress resets the backoff: due at once
    sells = await h.sells()
    assert [s.qty for s in sells] == [D("1"), D("1"), D("0.5")]
    h.bar(BTC, 95.0)
    await h.sync()
    pos = await h.position(pid)
    assert (pos.closed_at is not None, pos.exit_reason) == (True, "stop")
    await h.pm.step()  # a closed position is forgotten, never exited again
    assert len(await h.sells()) == 3


async def test_a_restart_honours_the_latch_and_never_double_sells(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    (sell,) = await h.sells()
    assert sell.status == "new"
    limit = (await h.position(pid)).time_limit_at
    short = PROFILE.model_copy(
        update={"risk": PROFILE.risk.model_copy(update={"max_hold_min": 10})}
    )
    pm2 = h.new_pm(profile=short)
    await pm2.step()
    await pm2.step()
    assert len(await h.sells()) == 1
    assert (await h.position(pid)).time_limit_at == limit
    h.bar(BTC, 95.0)
    await h.sync()
    assert (await h.position(pid)).closed_at is not None


async def test_a_restart_re_attempts_at_once(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=RejectingClose))
    await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    await h.pm.step()  # backoff 1 s: nothing
    assert len(await h.sells()) == 1
    await h.new_pm().step()
    assert [s.status for s in await h.sells()] == ["rejected", "rejected"]


async def test_failed_exits_back_off_and_alert_after_three(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=UnavailableClose))
    pid = await h.open_position()
    h.quote(BTC, 95.0)
    counts: list[int] = []
    with capture_logs() as logs:
        for t in (0.0, 0.5, 1.0, 2.0, 3.0):
            h.clock.now = T0 + timedelta(seconds=t)
            await h.pm.step()
            counts.append(len(await h.sells()))
    assert counts == [1, 1, 2, 2, 3]
    (failing,) = named(logs, "positions.exit_failing")
    assert (failing["failures"], failing["position_id"], failing["status"]) == (
        3,
        pid,
        "not_submitted",
    )
    assert failing["reasons"] == ["exit: stop"]


async def test_an_unknown_exit_without_a_broker_id_is_never_retried(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=LostClose))
    await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    with capture_logs() as logs:
        for t in (1, 30, 61):
            h.clock.now = T0 + timedelta(seconds=t)
            await h.pm.step()
    (sell,) = await h.sells()
    assert (sell.status, sell.broker_order_id) == ("unknown", None)
    lines = named(logs, "positions.exit_unresolved")
    assert len(lines) == 2
    assert all(line["order_rows"] == [sell.id] and not line["external"] for line in lines)


async def test_an_in_flight_exit_alerts_when_stuck(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    with capture_logs() as logs:
        h.clock.now = T0 + timedelta(seconds=30)
        await h.pm.step()
        assert not named(logs, "positions.exit_stuck")
        h.clock.now = T0 + timedelta(seconds=61)
        await h.pm.step()
    (stuck,) = named(logs, "positions.exit_stuck")
    assert stuck["order_rows"] == [(await h.sells())[0].id]
    assert len(await h.sells()) == 1


async def test_dust_holds_back_until_a_late_fill_rearms_the_latched_exit(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(BASE_FEES))
    h.sim.script(BTC, "buy", FillPlan((D("0.0001"),)))
    pid = await h.open_position()
    assert (await h.position(pid)).qty == D("0.00009975")
    h.quote(BTC, 95.0)
    with capture_logs() as logs:
        await h.pm.step()
    assert await h.sells() == []
    assert len(named(logs, "positions.exit_dust")) == 1
    assert await exit_decisions(sessions) == 1
    await h.pm.step()
    assert await exit_decisions(sessions) == 1
    h.bar(BTC, 95.0)  # the entry remainder fills
    await h.sync()
    await h.pm.step()
    assert len(await h.sells()) == 1
    assert len(await event_payloads(sessions, "ExitTriggered")) == 1
    await h.pm.step()
    assert len(await h.sells()) == 1
    h.bar(BTC, 95.0)
    await h.sync()
    pos = await h.position(pid)
    assert (pos.closed_at is not None, pos.exit_reason) == (True, "stop")


async def test_dust_while_the_journal_holds_a_sellable_qty_counts_as_a_failure(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=DustClose))
    await h.open_position()
    h.quote(BTC, 95.0)
    with capture_logs() as logs:
        for t in (0, 1, 3):
            h.clock.now = T0 + timedelta(seconds=t)
            await h.pm.step()
    assert len(await h.sells()) == 3
    assert not named(logs, "positions.exit_dust")
    (failing,) = named(logs, "positions.exit_failing")
    assert failing["failures"] == 3


async def test_a_broker_that_cannot_trade_latches_but_attempts_nothing(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=CannotTrade))
    pid = await h.open_position()
    h.quote(BTC, 95.0)
    with capture_logs() as logs:
        await h.pm.step()
        await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "stop"
    assert await h.sells() == []
    assert len(named(logs, "positions.broker_cannot_trade")) == 1


async def test_an_unprotected_position_is_reported_once_and_still_times_out(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position(stop=None, tp=None)
    with capture_logs() as logs:
        await h.pm.step()
        await h.pm.step()
    assert len(named(logs, "positions.unprotected")) == 1
    limit = (await h.position(pid)).time_limit_at
    assert limit is not None
    h.clock.now = limit
    await h.pm.step()
    assert (await h.position(pid)).exit_trigger == "time"


async def test_stale_held_alerts_once_per_episode(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    await h.open_position()
    with capture_logs() as logs:
        h.view.stale = {BTC}
        await h.pm.step()
        await h.pm.step()
        assert len(named(logs, "positions.stale_held")) == 1
        h.view.stale = set()
        await h.pm.step()
        h.view.stale = {BTC}
        await h.pm.step()
    assert len(named(logs, "positions.stale_held")) == 2


async def test_a_failing_position_never_stops_the_others(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus, engine_cls=BtcExitRaises)
    await h.open_position(BTC)
    await h.open_position(ETH)
    h.quote(BTC, 95.0)
    h.quote(ETH, 95.0)
    with capture_logs() as logs:
        await h.pm.step()
    (failed,) = named(logs, "positions.step_failed")
    assert failed["symbol"] == BTC
    assert len(await h.sells(ETH)) == 1


async def test_an_uncertain_latch_commit_is_retried_next_step(db_url: str) -> None:
    engine = make_engine(db_url)
    hook = CommitHook()
    sessions = hooked_sessionmaker(engine, hook)
    bus = EventBus(sessions)
    try:
        h = Harness(sessions, bus)
        pid = await h.open_position()
        h.quote(BTC, 100.0)
        await h.pm.step()  # writes time_limit_at
        h.quote(BTC, 95.0)
        hook.fail_before = 1
        with capture_logs() as logs:
            await h.pm.step()
        assert len(named(logs, "positions.latch_uncertain")) == 1
        assert (await h.position(pid)).exit_trigger is None
        await h.pm.step()
        assert (await h.position(pid)).exit_trigger == "stop"
    finally:
        await bus.close()
        await engine.dispose()


# --- paths the dry run left unreached ---------------------------------------------------


async def test_a_settling_exit_waits_for_the_ledger(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    (sell,) = await h.sells()
    h.bar(BTC, 95.0)  # filled at the sim
    async with sessions() as s:  # the status read landed, the fills are not booked yet
        await repo.update_order(s, sell.id, status="filled", now=h.clock.now)
    await h.pm.step()
    assert len(await h.sells()) == 1
    assert await exit_decisions(sessions) == 1


async def test_a_fully_sold_position_is_not_exited_again(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    h.sim.script(BTC, "buy", FillPlan((D("0.5"), D("0.00001"))))
    pid = await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    h.bar(BTC, 95.0)  # the sell fills 0.5; the entry's slice rounds to 0
    await h.sync()
    pos = await h.position(pid)
    assert (pos.qty, pos.closed_at) == (D("0"), None)
    await h.pm.step()
    assert await exit_decisions(sessions) == 1
    assert len(await h.sells()) == 1


async def test_an_unknown_trigger_value_is_a_step_failure(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    btc = await h.open_position(BTC)
    await h.open_position(ETH)
    async with sessions() as s:
        await s.execute(update(m.Position).where(m.Position.id == btc).values(exit_trigger="bogus"))
        await s.commit()
    h.quote(ETH, 95.0)
    with capture_logs() as logs:
        await h.pm.step()
    (failed,) = named(logs, "positions.step_failed")
    assert (failed["position_id"], failed["symbol"]) == (btc, BTC)
    assert await h.sells(BTC) == []
    assert len(await h.sells(ETH)) == 1
