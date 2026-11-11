"""AdmissionReadiness (WP 1.5d-3, spec 2026-11-11 3.2-3.3) with an injected clock and fake
risk/ledger/journal reads, and `journal_holdings` on a migrated SQLite journal."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, MutableMapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.core.admission import WAITING_REPEAT_S, AdmissionReadiness, journal_holdings
from autotrader.core.ledger import CashGuard
from autotrader.core.position_manager import PositionCheck, PositionOutcome, StepReport
from autotrader.journal import models as m
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

BTC = "BTC/USD"
ETH = "ETH/USD"
T0 = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
G = (5, 7)
FAULT = "not yet reconciled"


class Mono:
    """The tracker's monotonic clock; only the test advances it."""

    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, s: float) -> None:
        self.t += s


class FakeRisk:
    def __init__(self) -> None:
        self.cash_fault: str | None = FAULT
        self.holdings: dict[str, D] = {}
        self.holdings_error: Exception | None = None
        self.reads = 0

    async def broker_holdings(self) -> dict[str, D]:
        self.reads += 1
        if self.holdings_error is not None:
            raise self.holdings_error
        return dict(self.holdings)


class FakeLedger:
    """Answers `cash_guard()` from a queue, then repeats `default`. A queued exception is
    raised; a queued `asyncio.Event` is awaited first (a barrier), then the next entry is
    answered."""

    def __init__(self) -> None:
        self.default = CashGuard(G, unsettled=False)
        self.queue: list[CashGuard | Exception | asyncio.Event] = []
        self.entered = asyncio.Event()
        self.reads = 0

    async def cash_guard(self) -> CashGuard:
        self.reads += 1
        while self.queue:
            item = self.queue.pop(0)
            if isinstance(item, asyncio.Event):
                self.entered.set()
                await item.wait()
                continue
            if isinstance(item, Exception):
                raise item
            return item
        return self.default


class FakeJournal:
    def __init__(self) -> None:
        self.value: dict[str, tuple[D, D]] = {}
        self.error: Exception | None = None

    async def __call__(self) -> Mapping[str, tuple[D, D]]:
        if self.error is not None:
            raise self.error
        return dict(self.value)


class Rig:
    def __init__(self, *, waiting_after_s: float = 180.0, disabled: Sequence[str] = ()) -> None:
        self.clock = Mono()
        self.risk = FakeRisk()
        self.ledger = FakeLedger()
        self.journal = FakeJournal()
        self.tracker = AdmissionReadiness(
            self.risk,
            self.ledger,
            self.journal,
            waiting_after_s=waiting_after_s,
            disabled=disabled,
            clock=self.clock,
        )

    async def manage(self, *outcomes: PositionOutcome) -> StepReport:
        report = StepReport(tuple(PositionCheck(i + 1, BTC, o) for i, o in enumerate(outcomes)))

        async def step() -> StepReport:
            return report

        await self.tracker.manage(step)
        return report


def events(logs: list[MutableMapping[str, Any]], name: str) -> list[MutableMapping[str, Any]]:
    return [e for e in logs if e["event"] == name]


@pytest.fixture
def rig() -> Rig:
    return Rig()


async def test_opens_only_when_all_conditions_hold(rig: Rig) -> None:
    assert await rig.tracker.ready() is False  # the cash fault is set
    rig.risk.cash_fault = None
    assert await rig.tracker.ready() is False  # records T_b; no step since
    await rig.manage("evaluated", "latched")
    assert await rig.tracker.ready() is True


async def test_report_from_a_step_started_before_t_b_does_not_count(rig: Rig) -> None:
    release = asyncio.Event()

    async def slow() -> StepReport:
        await release.wait()
        return StepReport()

    task = asyncio.create_task(rig.tracker.manage(slow))
    await asyncio.sleep(0)  # the step has started (and taken its counter value)
    rig.risk.cash_fault = None
    assert await rig.tracker.ready() is False  # T_b is after the step's start
    release.set()
    await task
    assert rig.tracker.last_report == StepReport()
    assert await rig.tracker.ready() is False
    await rig.manage()
    assert await rig.tracker.ready() is True


@pytest.mark.parametrize("outcome", ["no_bid", "failed"])
async def test_incomplete_reports_do_not_count(rig: Rig, outcome: PositionOutcome) -> None:
    rig.risk.cash_fault = None
    assert await rig.tracker.ready() is False
    await rig.manage("evaluated", outcome)
    assert await rig.tracker.ready() is False
    await rig.manage("evaluated", "evaluated")
    assert await rig.tracker.ready() is True


async def test_empty_report_counts(rig: Rig) -> None:
    rig.risk.cash_fault = None
    assert await rig.tracker.ready() is False
    await rig.manage()
    assert await rig.tracker.ready() is True


async def test_failing_step_reraises_and_keeps_the_previous_report(rig: Rig) -> None:
    rig.risk.cash_fault = None
    assert await rig.tracker.ready() is False
    kept = await rig.manage("evaluated")

    async def boom() -> StepReport:
        raise RuntimeError("step")

    with pytest.raises(RuntimeError):
        await rig.tracker.manage(boom)
    assert rig.tracker.last_report == kept
    assert await rig.tracker.ready() is True


async def test_fault_set_again_resets_t_b(rig: Rig) -> None:
    rig.risk.cash_fault = None
    assert await rig.tracker.ready() is False
    await rig.manage("evaluated")
    rig.risk.cash_fault = "cash check failed"
    assert await rig.tracker.ready() is False
    rig.risk.cash_fault = None
    assert await rig.tracker.ready() is False  # the old report predates the new T_b
    await rig.manage("evaluated")
    assert await rig.tracker.ready() is True


async def test_fault_set_again_resets_t_b_without_a_cached_pass(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.journal.value = {BTC: (D(1), D(0))}  # short: no pass is cached
    assert await rig.tracker.ready() is False
    await rig.manage("evaluated")
    rig.risk.cash_fault = "cash check failed"
    reads = rig.risk.reads
    assert await rig.tracker.ready() is False
    assert rig.risk.reads == reads  # no holdings check while the fault is set
    rig.risk.cash_fault = None
    rig.risk.holdings = {BTC: D(1)}
    assert await rig.tracker.ready() is False  # the old report predates the new T_b
    await rig.manage("evaluated")
    assert await rig.tracker.ready() is True


async def test_holdings_shortfall_keeps_it_closed_and_logs_once_per_figures(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.journal.value = {BTC: (D(1), D(0))}
    rig.risk.holdings = {BTC: D("0.5")}
    await rig.manage()
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
        await rig.manage()
        assert await rig.tracker.ready() is False
    (line,) = events(logs, "admission.holdings_short")
    assert line["log_level"] == "error"
    assert (line["symbol"], line["journal_qty"], line["dust"], line["broker_qty"]) == (
        BTC,
        "1",
        "0",
        "0.5",
    )
    rig.risk.holdings = {BTC: D("0.4")}
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
        assert await rig.tracker.ready() is False
    (line,) = events(logs, "admission.holdings_short")
    assert line["broker_qty"] == "0.4"
    rig.risk.holdings = {BTC: D(1)}
    assert await rig.tracker.ready() is True


async def test_a_discarded_read_keeps_the_logged_figures(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.journal.value = {BTC: (D(1), D(0))}
    rig.risk.holdings = {BTC: D("0.5")}
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False  # short
        rig.ledger.queue = [CashGuard(G, unsettled=True)]  # discarded: figures kept
        assert await rig.tracker.ready() is False
        rig.risk.holdings = {BTC: D("0.5"), ETH: D(1)}
        rig.journal.value = {BTC: (D(1), D(0)), ETH: (D(2), D(0))}
        assert await rig.tracker.ready() is False  # BTC unchanged, ETH new
    assert [e["symbol"] for e in events(logs, "admission.holdings_short")] == [BTC, ETH]


async def test_a_symbol_no_longer_short_is_logged_again_when_it_returns() -> None:
    rig = Rig()
    rig.risk.cash_fault = None
    rig.journal.value = {BTC: (D(1), D(0)), ETH: (D(1), D(0))}
    rig.risk.holdings = {BTC: D("0.5"), ETH: D(0)}
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
        rig.risk.holdings = {BTC: D(1), ETH: D(0)}  # BTC recovers, ETH stays short
        assert await rig.tracker.ready() is False
        rig.risk.holdings = {BTC: D("0.5"), ETH: D(0)}  # the same BTC shortfall returns
        assert await rig.tracker.ready() is False
    assert [e["symbol"] for e in events(logs, "admission.holdings_short")] == [BTC, ETH, BTC]


async def test_broker_holding_above_the_journal_passes(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.journal.value = {BTC: (D(1), D("0.001"))}
    rig.risk.holdings = {BTC: D(3), ETH: D(5)}  # manual coins are legitimate
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False  # T_b, passed; no step since
        await rig.manage()
        assert await rig.tracker.ready() is True
    assert events(logs, "admission.holdings_short") == []


async def test_dust_counts_on_the_journal_side(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.journal.value = {BTC: (D(1), D("0.001"))}
    rig.risk.holdings = {BTC: D(1)}
    await rig.manage()
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    (line,) = events(logs, "admission.holdings_short")
    assert (line["journal_qty"], line["dust"], line["broker_qty"]) == ("1", "0.001", "1")


async def test_dust_only_symbol_without_broker_holding_is_short(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.journal.value = {ETH: (D(0), D("0.001"))}
    rig.risk.holdings = {}
    await rig.manage()
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    (line,) = events(logs, "admission.holdings_short")
    assert (line["symbol"], line["journal_qty"], line["dust"], line["broker_qty"]) == (
        ETH,
        "0",
        "0.001",
        "0",
    )


@pytest.mark.parametrize(
    "guards",
    [
        [CashGuard(G, unsettled=True), CashGuard(G, unsettled=False)],
        [CashGuard(G, unsettled=False), CashGuard(G, unsettled=True)],
        [CashGuard(G, unsettled=False), CashGuard((5, 8), unsettled=False)],
    ],
    ids=["g1_unsettled", "g2_unsettled", "generation_moved"],
)
async def test_unsettled_or_moved_guard_discards_silently(
    rig: Rig, guards: list[CashGuard]
) -> None:
    rig.risk.cash_fault = None
    rig.journal.value = {BTC: (D(1), D(0))}
    rig.risk.holdings = {BTC: D("0.5")}  # would be short if compared
    rig.ledger.queue = list(guards)
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    await rig.manage()
    assert events(logs, "admission.holdings_short") == []
    assert events(logs, "admission.holdings_read_failed") == []
    rig.risk.holdings = {BTC: D(1)}
    assert await rig.tracker.ready() is True


async def test_cached_holdings_pass_does_not_bypass_a_later_exit(rig: Rig) -> None:
    rig.risk.cash_fault = None
    assert await rig.tracker.ready() is False  # T_b, holdings pass on G, no report yet
    reads = rig.risk.reads
    assert reads == 1
    rig.ledger.default = CashGuard((6, 7), unsettled=True)  # the exit the step placed
    await rig.manage("evaluated")
    assert await rig.tracker.ready() is False
    rig.ledger.default = CashGuard((7, 8), unsettled=False)  # the exit settled
    assert await rig.tracker.ready() is False  # the stale pass is dropped
    assert rig.risk.reads == reads  # not re-run yet
    assert await rig.tracker.ready() is True  # re-run, passed on the new generation
    assert rig.risk.reads == reads + 1


async def _armed(rig: Rig) -> asyncio.Event:
    """T_b set, holdings passed on G, a complete eligible report; the next final guard read
    waits on the returned barrier."""
    rig.risk.cash_fault = None
    rig.ledger.queue = [
        CashGuard(G, unsettled=False),  # g1
        CashGuard(G, unsettled=False),  # g2
        CashGuard(G, unsettled=True),  # g3: not ready, the pass stays cached
    ]
    assert await rig.tracker.ready() is False
    await rig.manage("evaluated")
    barrier = asyncio.Event()
    rig.ledger.queue = [barrier, CashGuard(G, unsettled=False)]
    rig.ledger.entered.clear()
    return barrier


@pytest.mark.parametrize("change", ["fault", "report"])
async def test_final_guard_rechecks_the_fault_and_report_after_its_await(
    rig: Rig, change: str
) -> None:
    barrier = await _armed(rig)
    reads = rig.risk.reads
    task = asyncio.create_task(rig.tracker.ready())
    await rig.ledger.entered.wait()  # g3 is in flight; the generation will not move
    if change == "fault":
        rig.risk.cash_fault = "cash check failed"
    else:
        await rig.manage("evaluated", "failed")
    barrier.set()
    assert await task is False
    if change == "fault":
        rig.risk.cash_fault = None
        assert await rig.tracker.ready() is False  # the old report and pass no longer count
        assert rig.risk.reads == reads + 1  # the holdings check ran again
    await rig.manage("evaluated", "evaluated")
    assert await rig.tracker.ready() is True


async def test_final_guard_read_failure_is_a_failed_holdings_read(rig: Rig) -> None:
    await _t_b_then_step(rig)
    rig.clock.advance(180)
    rig.ledger.queue = [
        CashGuard(G, unsettled=False),
        CashGuard(G, unsettled=False),
        RuntimeError("guard"),
    ]
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    assert len(events(logs, "admission.holdings_read_failed")) == 1
    (line,) = events(logs, "cycle.admission_waiting")
    assert line["unmet"] == ["holdings check not completed"]
    assert await rig.tracker.ready() is True  # the cached pass and the step still count


async def test_broker_read_failure_logs_once_per_streak(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.risk.holdings_error = RuntimeError("broker down")
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
        await rig.manage()
        assert await rig.tracker.ready() is False
        rig.risk.holdings_error = None
        rig.journal.value = {BTC: (D(1), D(0))}  # a completed read, short: not ready
        assert await rig.tracker.ready() is False
        rig.risk.holdings_error = RuntimeError("broker down")
        assert await rig.tracker.ready() is False
    failed = events(logs, "admission.holdings_read_failed")
    assert len(failed) == 2
    assert failed[0]["log_level"] == "warning"
    assert "broker down" in failed[0]["error"]


@pytest.mark.parametrize("where", ["g1", "journal"])
async def test_guard_and_journal_read_failures_are_retried(rig: Rig, where: str) -> None:
    rig.risk.cash_fault = None
    if where == "g1":
        rig.ledger.queue = [RuntimeError("guard")]
    else:
        rig.journal.error = RuntimeError("journal")
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    await rig.manage()
    assert len(events(logs, "admission.holdings_read_failed")) == 1
    rig.journal.error = None
    assert await rig.tracker.ready() is True


async def _not_ready_without_t_b(rig: Rig) -> None:
    pass  # the cash fault stays set


async def _not_ready_with_cached_pass(rig: Rig) -> None:
    rig.risk.cash_fault = None
    assert await rig.tracker.ready() is False  # T_b and a holdings pass
    await rig.manage("no_bid")


@pytest.mark.parametrize(
    "setup", [_not_ready_without_t_b, _not_ready_with_cached_pass], ids=["fault", "cached_pass"]
)
async def test_waiting_logged_after_three_snapshot_intervals_and_repeated(
    rig: Rig, setup: Callable[[Rig], Awaitable[None]]
) -> None:
    await setup(rig)
    with capture_logs() as logs:
        rig.clock.advance(179)
        assert await rig.tracker.ready() is False
        assert events(logs, "cycle.admission_waiting") == []
        rig.clock.advance(1)
        assert await rig.tracker.ready() is False
        assert len(events(logs, "cycle.admission_waiting")) == 1
        rig.clock.advance(WAITING_REPEAT_S - 1)
        assert await rig.tracker.ready() is False
        assert len(events(logs, "cycle.admission_waiting")) == 1
        rig.clock.advance(1)
        assert await rig.tracker.ready() is False
    waiting = events(logs, "cycle.admission_waiting")
    assert len(waiting) == 2
    assert waiting[0]["log_level"] == "warning"
    assert waiting[0]["unmet"]


async def test_waiting_names_unsettled_rows_while_the_fault_is_set(rig: Rig) -> None:
    rig.ledger.default = CashGuard(G, unsettled=True)
    rig.clock.advance(180)
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    (line,) = events(logs, "cycle.admission_waiting")
    assert line["unmet"] == ["cash check not completed: not yet reconciled", "unsettled order rows"]


async def test_waiting_omits_unsettled_rows_when_the_guard_read_raises(rig: Rig) -> None:
    rig.ledger.queue = [RuntimeError("guard")]
    rig.clock.advance(180)
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    (line,) = events(logs, "cycle.admission_waiting")
    assert line["unmet"] == ["cash check not completed: not yet reconciled"]


async def test_waiting_names_the_unmet_conditions_after_t_b(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.clock.advance(180)
    rig.risk.holdings_error = RuntimeError("broker down")
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False  # no step, holdings read failed
        rig.risk.holdings_error = None
        await rig.manage("evaluated")
        rig.ledger.default = CashGuard(G, unsettled=True)
        rig.clock.advance(WAITING_REPEAT_S)
        assert await rig.tracker.ready() is False  # discarded: no step issue left
    first, second = events(logs, "cycle.admission_waiting")
    assert first["unmet"] == [
        "no position manager step since the cash check",
        "holdings check not completed",
    ]
    assert second["unmet"] == ["holdings check not completed"]


async def _t_b_then_step(rig: Rig) -> None:
    """Records T_b (its holdings read is discarded), then an eligible complete step."""
    rig.risk.cash_fault = None
    rig.ledger.queue = [CashGuard(G, unsettled=True)]
    assert await rig.tracker.ready() is False
    await rig.manage()


async def test_waiting_names_unsettled_rows_at_the_final_guard(rig: Rig) -> None:
    await _t_b_then_step(rig)
    rig.clock.advance(180)
    rig.ledger.queue = [
        CashGuard(G, unsettled=False),
        CashGuard(G, unsettled=False),
        CashGuard(G, unsettled=True),
    ]
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    (line,) = events(logs, "cycle.admission_waiting")
    assert line["unmet"] == ["unsettled order rows"]


async def test_waiting_after_a_moved_generation_names_the_holdings_check(rig: Rig) -> None:
    await _t_b_then_step(rig)
    rig.clock.advance(180)
    rig.ledger.queue = [
        CashGuard(G, unsettled=False),
        CashGuard(G, unsettled=False),
        CashGuard((9, 9), unsettled=False),
    ]
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    (line,) = events(logs, "cycle.admission_waiting")
    assert line["unmet"] == ["holdings check not completed"]


async def test_waiting_not_logged_when_ready_earlier(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.clock.advance(100)
    assert await rig.tracker.ready() is False
    await rig.manage()
    with capture_logs() as logs:
        assert await rig.tracker.ready() is True
        rig.clock.advance(1000)
        assert await rig.tracker.ready() is True
    assert events(logs, "cycle.admission_waiting") == []


async def test_waiting_log_names_positions_and_short_symbols(rig: Rig) -> None:
    rig.risk.cash_fault = None
    rig.journal.value = {BTC: (D(1), D(0))}
    rig.risk.holdings = {BTC: D("0.5")}

    async def step() -> StepReport:
        return StepReport(
            (PositionCheck(3, BTC, "evaluated"), PositionCheck(7, "DOGE/USD", "no_bid"))
        )

    assert await rig.tracker.ready() is False
    await rig.tracker.manage(step)
    rig.clock.advance(180)
    with capture_logs() as logs:
        assert await rig.tracker.ready() is False
    (line,) = events(logs, "cycle.admission_waiting")
    assert line["positions"] == [{"position_id": 7, "symbol": "DOGE/USD", "outcome": "no_bid"}]
    assert line["short"] == [BTC]
    assert line["unmet"] == ["positions not evaluated", "holdings short"]


async def test_disabled_tracker_never_opens_and_never_logs() -> None:
    rig = Rig(disabled=("broker cannot place orders",))
    rig.risk.cash_fault = None
    rig.journal.value = {BTC: (D(1), D(0))}
    await rig.manage()
    with capture_logs() as logs:
        for _ in range(3):
            assert await rig.tracker.ready() is False
            rig.clock.advance(WAITING_REPEAT_S)
    assert logs == []
    assert rig.risk.reads == 0 and rig.ledger.reads == 0


# --- journal_holdings on a real journal ---------------------------------------------------


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


def _position(symbol: str, qty: str, *, closed: bool = False) -> m.Position:
    return m.Position(
        symbol=symbol,
        side="long",
        qty=D(qty),
        entry_price=D("100"),
        opened_at=T0,
        closed_at=T0 if closed else None,
        fees=D(0),
        dust_consumed=D(0),
    )


async def test_journal_holdings_sums_open_positions_and_dust(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with sessions() as s:
        s.add_all(
            [
                _position(BTC, "0.5"),
                _position(BTC, "0.25"),
                _position(BTC, "9", closed=True),
                m.SymbolDust(symbol=ETH, qty=D("0.00005"), updated_at=T0),
                m.SymbolDust(symbol="SOL/USD", qty=D(0), updated_at=T0),  # no balance
            ]
        )
        await s.commit()
    assert await journal_holdings(sessions) == {
        BTC: (D("0.75"), D(0)),
        ETH: (D(0), D("0.00005")),
    }


async def test_journal_holdings_combines_a_position_and_dust_on_one_symbol(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with sessions() as s:
        s.add_all([_position(BTC, "1"), m.SymbolDust(symbol=BTC, qty=D("0.001"), updated_at=T0)])
        await s.commit()
    assert await journal_holdings(sessions) == {BTC: (D(1), D("0.001"))}
