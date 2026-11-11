"""Restart scenarios on the real engine, ledger, Position Manager and runner (WP 1.5d-3,
spec 2026-11-11 section 7 tests 15-18). SimBroker, injected quotes, no loop runs: each test
drives the steps `run core` supervises (`sync`, `snapshot`, `manage`, `admit`) in an explicit
order. Exits placed after `T_b` must settle before admission opens (plan, Known costs), so a
test that expects admission to open fills the exit on a bar once the sell row exists."""

from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping
from datetime import datetime
from decimal import Decimal as D
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.broker.base import BrokerOrder
from autotrader.core.admission import AdmissionReadiness, journal_holdings
from autotrader.core.bus import EventBus
from autotrader.core.cycle import CycleRunner
from autotrader.core.kill import PAUSED_KEY
from autotrader.core.position_manager import PositionManager
from autotrader.core.runtime import _admission_step, _snapshot_step, _sync_step
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import OrderResult
from autotrader.risk.resolve import entry_client_id
from autotrader.risk.types import ExitRequest
from tests.pm_harness import BTC, ETH, PROFILE, Harness, NoPrices, OpenGate

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


class Restarted:
    """What `start()` builds on the same journal: a recovered runner (admission closed), a
    fresh Position Manager and the readiness tracker. `tick()` runs the named steps."""

    def __init__(
        self, h: Harness, runner: CycleRunner, pm: PositionManager, readiness: AdmissionReadiness
    ) -> None:
        self.h, self.runner, self.pm, self.readiness = h, runner, pm, readiness

    async def tick(self, *order: str) -> None:
        h = self.h
        for name in order:
            if name == "sync":
                await _sync_step(h.engine, h.ledger)
            elif name == "snapshot":
                h.clock.advance(1)  # equity_snapshots.ts is unique; the Harness clock is fixed
                await _snapshot_step(h.engine, h.ledger)
            elif name == "manage":
                await self.readiness.manage(self.pm.step)
            elif name == "admit":
                await _admission_step(self.readiness, self.runner)
            else:
                raise ValueError(name)

    def outcomes(self) -> dict[int, str]:
        report = self.readiness.last_report
        assert report is not None
        return {p.position_id: p.outcome for p in report.positions}


async def restart(h: Harness) -> Restarted:
    runner = CycleRunner(
        profile=PROFILE,
        strategy_id=1,
        market=NoPrices(),
        gate=OpenGate(),
        risk=h.engine,
        bus=h.bus,
        sessions=h.sessions,
        clock=h.clock,
    )
    await runner.recover()
    readiness = AdmissionReadiness(
        h.engine,
        h.ledger,
        partial(journal_holdings, h.sessions),
        waiting_after_s=180,
        clock=lambda: 0.0,  # ordering uses the tracker's counter, not this clock
    )
    return Restarted(h, runner, h.new_pm(), readiness)


def names(logs: Logs) -> list[str]:
    return [str(line["event"]) for line in logs]


def index(logs: Logs, event: str) -> int:
    return names(logs).index(event)


async def cycle_of(h: Harness, row_id: int) -> int:
    async with h.sessions() as s:
        row = await s.get(m.Order, row_id)
        assert row is not None and row.cycle_id is not None
        return row.cycle_id


async def journal_state(h: Harness, cycle_ids: list[int]) -> dict[str, Any]:
    async with h.sessions() as s:
        states = [await repo.cycle_state(s, c) for c in cycle_ids]
        open_symbols = sorted(p.symbol for p in await repo.open_positions(s))
    return {"states": states, "open": open_symbols, "btc_sells": len(await h.sells(BTC))}


async def test_restart_reconciles_before_admission_opens(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    await h.open_position(BTC, stop="95")  # (c), registers the strategy for the cycle FK
    async with sessions() as s:  # (a): RISK_CHECK, no order row
        strategy = await repo.register_strategy(s, PROFILE, "yaml", now=h.clock.now)
        stuck = await repo.insert_cycle(
            s,
            strategy_id=strategy.id,
            symbol="SOL/USD",
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            now=h.clock.now,
        )
        await s.commit()
        stuck_id = stuck.id
    eth_row, _ = await h.submit_entry(ETH, limit=None)  # (b): fills while down, not booked
    h.bar(ETH, 100.0)
    eth_cycle = await cycle_of(h, eth_row)

    with capture_logs() as logs:
        r = await restart(h)
        h.quote(BTC, 90.0)
        h.quote(ETH, 100.0)
        before_open: dict[str, Any] | None = None
        filled = False
        for _ in range(6):
            await r.tick("sync", "snapshot", "manage")
            if not filled and await h.sells(BTC):
                h.bar(BTC, 90.0)
                filled = True
            state = await journal_state(h, [stuck_id, eth_cycle])
            await r.tick("admit")
            if r.runner.admission_open:
                before_open = state
                break

    assert r.runner.admission_open
    assert before_open == {"states": ["ERROR", "FILLED"], "open": [ETH], "btc_sells": 1}
    assert index(logs, "positions.exit_triggered") < index(logs, "cycle.admission_open")


async def test_position_booked_on_a_later_pass_waits_for_its_exit(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    h = Harness(sessions, bus)
    row_id, uncorrelated = await h.submit_entry(BTC, ack=False, limit=None, stop="95")
    cycle_id = await cycle_of(h, row_id)
    async with sessions() as s:  # the entry's response was lost: unknown, no broker id
        jid = await repo.journal_id(s)
        assert jid is not None
        cid = entry_client_id(jid, cycle_id)
        row = await s.get(m.Order, row_id)
        assert row is not None
        row.client_order_id, row.status = cid, "unknown"
        await s.commit()
    await h.sim.cancel_order(uncorrelated)
    await h.sim.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=D("1"), type="market", client_order_id=cid)
    )
    h.bar(BTC, 100.0)  # filled while the core was down
    h.clock.advance(71)  # past io_timeout_s + order_visibility_allowance_s

    with capture_logs() as logs:
        r = await restart(h)  # no BTC quote yet
        await r.tick("sync")  # pass 1 links the row
        async with sessions() as s:
            linked = await s.get(m.Order, row_id)
            assert linked is not None and linked.broker_order_id is not None
            assert await repo.open_positions(s) == []
        await r.tick("manage", "sync", "snapshot", "admit")  # manage: no position yet
        (pos,) = await _open_positions(h)
        await r.tick("manage", "admit")  # T_b was set by the previous admit
        assert r.outcomes() == {pos.id: "no_bid"}
        assert not r.runner.admission_open

        h.quote(BTC, 90.0)
        await r.tick("manage")  # latched, sell placed
        assert (await h.position(pos.id)).exit_trigger == "stop"
        assert len(await h.sells(BTC)) == 1
        h.bar(BTC, 90.0)
        await r.tick("sync", "snapshot", "admit")
        assert not r.runner.admission_open  # the pass was cached before the exit settled
        await r.tick("manage", "admit")

    assert r.runner.admission_open
    assert index(logs, "positions.exit_triggered") < index(logs, "cycle.admission_open")


async def _open_positions(h: Harness) -> list[m.Position]:
    async with h.sessions() as s:
        return await repo.open_positions(s)


async def test_swallowed_evaluation_failure_keeps_admission_closed(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = Harness(sessions, bus)
    btc = await h.open_position(BTC)
    eth = await h.open_position(ETH)
    r = await restart(h)
    h.quote(BTC, 100.0)
    h.quote(ETH, 100.0)
    evaluate = r.pm._evaluate
    failures = {"left": 2}

    async def flaky(pos: m.Position, time_limit: datetime) -> tuple[str | None, Any]:
        if pos.id == eth and failures["left"] > 0:
            failures["left"] -= 1
            raise RuntimeError("evaluation failed")
        return await evaluate(pos, time_limit)

    monkeypatch.setattr(r.pm, "_evaluate", flaky)
    await r.tick("sync", "snapshot", "admit")  # T_b before the failing steps
    for _ in range(2):
        await r.tick("manage")
        assert r.outcomes() == {btc: "evaluated", eth: "failed"}
        await r.tick("admit")
        assert not r.runner.admission_open
    await r.tick("manage")
    assert r.outcomes() == {btc: "evaluated", eth: "evaluated"}
    await r.tick("admit")
    assert r.runner.admission_open


async def test_holdings_shortfall_keeps_admission_closed(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position(BTC, qty="1", stop="95")
    # sold at the broker outside the journal while the core was down
    await h.sim.submit_order(BrokerOrder(symbol=BTC, side="sell", qty=D("1"), type="market"))
    h.bar(BTC, 100.0)

    exits: list[ExitRequest] = []
    real_exit: Callable[[ExitRequest], Awaitable[OrderResult]] = h.engine.exit

    async def spy(request: ExitRequest) -> OrderResult:
        exits.append(request)
        return await real_exit(request)

    monkeypatch.setattr(h.engine, "exit", spy)
    with capture_logs() as logs:
        r = await restart(h)
        calls_per_round: list[int] = []
        for _ in range(4):
            h.quote(BTC, 90.0)  # fresh, below the stop
            await r.tick("sync", "snapshot", "admit", "manage", "admit")
            calls_per_round.append(len(exits))
            assert not r.runner.admission_open
            h.clock.advance(31)  # past the exit backoff (capped at 30 s)

    assert (await h.position(pid)).exit_trigger == "stop"
    assert calls_per_round[0] >= 1 and calls_per_round[-1] > calls_per_round[0]
    short = [line for line in logs if line["event"] == "admission.holdings_short"]
    assert len(short) == 1
    assert (short[0]["symbol"], short[0]["journal_qty"], short[0]["broker_qty"]) == (BTC, "1", "0")
    assert "cycle.admission_open" not in names(logs)
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_KEY) is None
