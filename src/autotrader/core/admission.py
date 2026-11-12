"""Cycle Runner admission readiness (WP 1.5d-3; spec 2026-11-11 3.2-3.3, PLAN D21).

`AdmissionReadiness.ready()` says when the runner may open admission: (a) admission is not
disabled (static: the broker can place orders, the backfill and `recover()` succeeded); (b) the
engine's cash fault is clear, and the first clear observation records `T_b` (the proof that every
order the journal knows of was settled and booked); (c) a Position Manager step that started
after `T_b` reported every open position `evaluated` or `latched` (an empty report counts); (d)
the one-sided holdings check passed after `T_b`. (b) comes first, then (c) and (d). "After" is
decided by a private event counter that `T_b` and every step start draw from, never by the
clock (a clock that does not move would make a step start equal `T_b`); the injected monotonic
clock only times the `cycle.admission_waiting` log (after `waiting_after_s`, then every
`WAITING_REPEAT_S`). A cash fault seen set again before admission opens forgets `T_b` and the
holdings result, so (c) and (d) must hold again after the next clear observation.

The holdings check: per symbol with an open journal position or a `symbol_dust` balance,
`open qty + dust <= broker total qty` (a larger broker holding is legitimate and not checked). It
reads the ledger's cash guard before the broker read and again after the journal read. An
unsettled row at the first read skips the check without reading the broker; an unsettled row
at the second read or a moved generation discards the result. Both are silent (no log line of
their own), and `cycle.admission_waiting` then names `unsettled order rows`. A shortfall logs
`admission.holdings_short` once per figures; a raising read logs
`admission.holdings_read_failed` once per failure streak (the waiting log names `holdings check
not completed`). A pass is tied to the generation it passed on: before saying ready, the tracker
reads the guard once more, and after that read it decides with no await until `return True`
(the fault and the latest report are re-read there), and a moved generation re-runs the check.
So an exit placed after a cached pass must settle first only if it is in the journal by that
final guard read: the read closes its session and so yields to the loop a few turns before the
decision, and an exit committed in that gap is not seen. That gap is milliseconds, the exit
lowers the journal and the broker together, and `check()` rejects an entry on a symbol with an
unsettled sell (check 5a). Once ready, the caller opens admission for good (D21.3).
"""

import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from decimal import Decimal
from typing import Protocol

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.ledger import CashGuard
from autotrader.core.position_manager import StepReport
from autotrader.journal import repo

log = structlog.get_logger(__name__)

WAITING_REPEAT_S = 600.0  # cycle.admission_waiting repeats this often while admission is closed

_NO_STEP = "no position manager step since the cash check"
_NOT_EVALUATED = "positions not evaluated"
_SHORT = "holdings short"
_NOT_COMPLETED = "holdings check not completed"
_UNSETTLED = "unsettled order rows"


class ReadinessRisk(Protocol):
    cash_fault: str | None

    async def broker_holdings(self) -> dict[str, Decimal]: ...


class GuardSource(Protocol):
    async def cash_guard(self) -> CashGuard: ...


JournalHoldings = Callable[[], Awaitable[Mapping[str, tuple[Decimal, Decimal]]]]


async def journal_holdings(
    sessions: async_sessionmaker[AsyncSession],
) -> dict[str, tuple[Decimal, Decimal]]:
    """Per symbol with an open position or a dust balance: (sum of open positions.qty,
    symbol_dust qty), read in one session."""
    async with sessions() as session:
        positions = await repo.open_positions(session)
        dust = await repo.symbol_dust_balances(session)
    out: dict[str, tuple[Decimal, Decimal]] = {}
    for p in positions:
        qty, _ = out.get(p.symbol, (Decimal(0), Decimal(0)))
        out[p.symbol] = (qty + p.qty, Decimal(0))
    for symbol, balance in dust.items():
        qty, _ = out.get(symbol, (Decimal(0), Decimal(0)))
        out[symbol] = (qty, balance)
    return out


class AdmissionReadiness:
    def __init__(
        self,
        risk: ReadinessRisk,
        ledger: GuardSource,
        journal: JournalHoldings,
        *,
        waiting_after_s: float,
        disabled: Sequence[str] = (),
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._risk = risk
        self._ledger = ledger
        self._journal = journal
        self._waiting_after_s = waiting_after_s
        self._disabled = tuple(disabled)
        self._clock = clock
        self._created = clock()
        self._waiting_logged_at: float | None = None
        self._seq = 0
        self._t_b: int | None = None
        self._last: tuple[int, StepReport] | None = None
        self._passed: tuple[int, int] | None = None  # the generation the holdings check passed on
        self._short: list[str] = []  # short symbols of the last completed comparison
        self._skipped = False  # the last holdings check was skipped on an unsettled guard
        self._logged_short: dict[str, tuple[Decimal, Decimal, Decimal]] = {}
        self._read_failing = False

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    @property
    def last_report(self) -> StepReport | None:
        return None if self._last is None else self._last[1]

    async def manage(self, step: Callable[[], Awaitable[StepReport]]) -> None:
        """Run one Position Manager step and keep its report with its start; a raising step
        re-raises and keeps the previous report."""
        started = self._next_seq()
        report = await step()
        self._last = (started, report)

    def _reset(self) -> None:
        self._t_b = None
        self._passed = None
        self._short = []
        self._skipped = False

    def _eligible(self) -> bool:
        """(c) on the latest report: a step that started after `T_b` and is complete."""
        return (
            self._t_b is not None
            and self._last is not None
            and self._last[0] > self._t_b
            and self._last[1].complete
        )

    def _read_failed(self, error: Exception) -> None:
        if not self._read_failing:
            log.warning("admission.holdings_read_failed", error=repr(error))
        self._read_failing = True

    async def ready(self) -> bool:
        if self._disabled:
            return False
        now = self._clock()
        if self._risk.cash_fault is not None:
            self._reset()
        elif self._t_b is None:
            self._t_b = self._next_seq()
        note: str | None = None  # the holdings entry of the waiting log when a pass is cached
        if self._t_b is not None and self._passed is None:
            await self._check_holdings()
        if self._t_b is not None and self._passed is not None:
            try:
                g3 = await self._ledger.cash_guard()
            except Exception as e:
                self._read_failed(e)
                note = _NOT_COMPLETED
            else:
                self._read_failing = False
                # No await from here until `return True`: the fault and the report may have
                # changed while g3 was read, without moving the generation.
                if self._risk.cash_fault is not None:
                    self._reset()
                elif not self._eligible():
                    note = _UNSETTLED if g3.unsettled else None
                elif g3.unsettled:
                    note = _UNSETTLED
                elif g3.generation != self._passed:
                    self._passed = None
                else:
                    return True
        await self._waiting(now, note)
        return False

    async def _check_holdings(self) -> None:
        try:
            g1 = await self._ledger.cash_guard()
            if g1.unsettled:
                self._read_failing = False  # a completed read ends a failure streak
                self._skipped = True  # an order in flight: no broker read, retried next tick
                return
            broker = await self._risk.broker_holdings()
            journal = await self._journal()
            g2 = await self._ledger.cash_guard()
        except Exception as e:
            self._skipped = False
            self._read_failed(e)
            return
        self._read_failing = False
        if g2.unsettled or g1.generation != g2.generation:
            self._skipped = True  # an order in flight: retried on the next tick, no log
            return
        self._skipped = False
        short: dict[str, tuple[Decimal, Decimal, Decimal]] = {}
        for symbol, (open_qty, dust) in journal.items():
            held = broker.get(symbol, Decimal(0))
            if open_qty + dust > held:
                short[symbol] = (open_qty, dust, held)
        for symbol in [s for s in self._logged_short if s not in short]:
            del self._logged_short[symbol]
        for symbol, figures in short.items():
            if self._logged_short.get(symbol) != figures:
                open_qty, dust, held = figures
                log.error(
                    "admission.holdings_short",
                    symbol=symbol,
                    journal_qty=str(open_qty),
                    dust=str(dust),
                    broker_qty=str(held),
                )
                self._logged_short[symbol] = figures
        self._short = sorted(short)
        if not short:
            self._passed = g2.generation

    async def _waiting(self, now: float, note: str | None) -> None:
        """Log `cycle.admission_waiting` once `waiting_after_s` has passed, then every
        `WAITING_REPEAT_S` while admission stays closed."""
        if now - self._created < self._waiting_after_s:
            return
        if self._waiting_logged_at is not None and now - self._waiting_logged_at < WAITING_REPEAT_S:
            return
        self._waiting_logged_at = now
        unmet: list[str] = []
        fault = self._risk.cash_fault
        if self._t_b is None:
            unmet.append(f"cash check not completed: {fault}")
            try:
                guard = await self._ledger.cash_guard()
            except Exception:
                guard = None  # only omits the entry below
            if guard is not None and guard.unsettled:
                unmet.append(_UNSETTLED)
        else:
            if self._last is None or self._last[0] <= self._t_b:
                unmet.append(_NO_STEP)
            elif not self._last[1].complete:
                unmet.append(_NOT_EVALUATED)
            if self._passed is None:
                if self._skipped:
                    unmet.append(_UNSETTLED)
                else:
                    unmet.append(_SHORT if self._short else _NOT_COMPLETED)
            elif note is not None:
                unmet.append(note)
        positions = [] if self._last is None else self._last[1].positions
        log.warning(
            "cycle.admission_waiting",
            unmet=unmet,
            positions=[
                {"position_id": p.position_id, "symbol": p.symbol, "outcome": p.outcome}
                for p in positions
                if p.outcome not in ("evaluated", "latched")
            ],
            short=list(self._short),
        )
