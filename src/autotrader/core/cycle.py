"""Cycle Runner (SPEC §4.3; PLAN D15; spec 2026-10-11-cycle-runner-design.md).

One cycle per delivered `CandidateSignal` (the bus delivers at most once and never
replays). The `CandidateSignal` handler only admits: freshness, gate, then under the
admission lock the G14 caps and open entry orders, and the insert (`publish_tx`, so the
row and its `CycleTransitioned` event commit together). One asyncio task per cycle then
walks

    NEW -> ENRICHING -> ANALYZING -> DECIDING -> RISK_CHECK -> SUBMITTED -> FILLED

with REJECTED / EXPIRED / ERROR exits (§4). `check()` is called once, never retried,
and never timed out by the runner (only `close()` forces it). After submission only
proven outcomes are mapped; `new` / `unknown` / `canceled` stay SUBMITTED until WP 1.5
reconciles the order. Orders are reconciled by order row, not cycle state: an ERROR
cycle may own a real `pending` order.

Any `CommitUncertain` suspends admission for the rest of the process; `recover()` on
the next start resolves the uncertain rows (§9).
"""

import asyncio
import functools
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, TypeVar

import structlog
from pydantic import Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.agents import stub
from autotrader.agents.stub import StubBuy, StubSkip
from autotrader.core.bus import CommitUncertain, DomainEvent, EventBus
from autotrader.core.config import StrategyProfile
from autotrader.core.indicators import compute_features
from autotrader.core.kill import ControlStatus
from autotrader.core.logging import bind_cycle
from autotrader.core.resample import parse_timeframe
from autotrader.core.scanner import CandidateSignal
from autotrader.core.types import Bar, Quote
from autotrader.journal import repo
from autotrader.risk.engine import OrderResult
from autotrader.risk.types import EntryIntent

log = structlog.get_logger(__name__)
T = TypeVar("T")


class CycleState(StrEnum):
    NEW = "NEW"
    ENRICHING = "ENRICHING"
    ANALYZING = "ANALYZING"
    DECIDING = "DECIDING"
    RISK_CHECK = "RISK_CHECK"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    ERROR = "ERROR"


S = CycleState
TERMINAL = frozenset({S.FILLED, S.REJECTED, S.EXPIRED, S.ERROR})
# Nothing was sent to the broker in these states (recover() may end them).
BEFORE_CHECK = frozenset({S.NEW, S.ENRICHING, S.ANALYZING, S.DECIDING})

RUNNER_EDGES: frozenset[tuple[CycleState, CycleState]] = frozenset(
    {
        (S.NEW, S.ENRICHING),
        (S.ENRICHING, S.ANALYZING),
        (S.ENRICHING, S.EXPIRED),
        (S.ANALYZING, S.DECIDING),
        (S.DECIDING, S.RISK_CHECK),
        (S.DECIDING, S.REJECTED),
        (S.RISK_CHECK, S.SUBMITTED),
        (S.RISK_CHECK, S.REJECTED),
        (S.RISK_CHECK, S.EXPIRED),
        (S.SUBMITTED, S.FILLED),  # shared with reconciliation
        (S.SUBMITTED, S.REJECTED),
        *((s, S.ERROR) for s in S if s not in TERMINAL),
    }
)
# Taken only by WP 1.5 reconciliation, from the broker's filled qty (§4).
RECONCILIATION_ONLY_EDGES: frozenset[tuple[CycleState, CycleState]] = frozenset(
    {(S.SUBMITTED, S.EXPIRED), (S.RISK_CHECK, S.FILLED)}
)


def check_edge(from_: CycleState | None, to: CycleState, *, reconciliation: bool = False) -> None:
    if from_ is None:
        ok = to is S.NEW
    else:
        edges = RUNNER_EDGES | RECONCILIATION_ONLY_EDGES if reconciliation else RUNNER_EDGES
        ok = (from_, to) in edges
    if not ok:
        raise ValueError(f"illegal cycle transition {from_} -> {to}")


class CycleTransitioned(DomainEvent):
    """One persisted state change. `details` (not `payload`: that is the bus's method)."""

    from_state: CycleState | None
    to_state: CycleState
    details: dict[str, Any] = Field(default_factory=dict)


def signal_is_stale(bar_ts: datetime, timeframe: str, now: datetime) -> bool:
    """The Scanner's obsolete-bar rule (D13): more than one period since the bar ended."""
    period = parse_timeframe(timeframe)
    return now - (bar_ts + period) > period


def signal_features(candidate: CandidateSignal) -> dict[str, Any]:
    """`cycles.signal_features`: the features plus the candidate's identity in time."""
    return {
        **candidate.features,
        "_timeframe": candidate.timeframe,
        "_bar_ts": candidate.bar_ts.isoformat(),
    }


STEP_TIMEOUT_S = 30.0  # SPEC §4.3: e.g. an LLM call > 30 s -> ERROR
NO_ANALYST = {"skipped": "no analyst in phase 1"}


class CycleMarketView(Protocol):
    def history(self, symbol: str, timeframe: str) -> list[Bar]: ...
    def quote(self, symbol: str) -> Quote | None: ...
    def last_price(self, symbol: str) -> float | None: ...
    def is_stale(self, symbol: str) -> bool: ...


class Gate(Protocol):
    async def status(self, now: datetime | None = None) -> ControlStatus: ...


class EntryChecker(Protocol):
    async def check(self, intent: EntryIntent) -> OrderResult: ...


@dataclass(frozen=True)
class CycleSnapshot:
    ask: Decimal
    bid: Decimal | None
    last_price: Decimal | None
    atr: Decimal

    def details(self) -> dict[str, Any]:
        return {
            "ask": str(self.ask),
            "bid": None if self.bid is None else str(self.bid),
            "last_price": None if self.last_price is None else str(self.last_price),
            "atr": str(self.atr),
        }


Decision = StubBuy | StubSkip
DecisionMaker = Callable[[CandidateSignal, CycleSnapshot], Awaitable[Decision]]


@dataclass(frozen=True)
class CloseReport:
    abandoned: tuple[int, ...] = ()  # cycle ids whose task or check() was still running


@dataclass
class _Cursor:
    cycle_id: int
    state: CycleState


class _WriteFailed(Exception):
    """A transition write failed; the database error is the `__cause__`."""


def _dec(value: float | None) -> Decimal | None:
    return None if value is None or not math.isfinite(value) else Decimal(str(value))


async def _verify(session: AsyncSession, event: DomainEvent) -> bool:
    """Read-back after a commit error: is the cycle row in the event's target state?"""
    if not isinstance(event, CycleTransitioned) or event.cycle_id is None:
        raise TypeError(f"cannot verify {type(event).__name__}")
    return await repo.cycle_state(session, event.cycle_id) == event.to_state


def _consume(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()  # retrieved: a check() nobody awaits any more must not warn


class CycleRunner:
    def __init__(
        self,
        *,
        profile: StrategyProfile,
        strategy_id: int,
        market: CycleMarketView,
        gate: Gate,
        risk: EntryChecker,
        bus: EventBus,
        sessions: async_sessionmaker[AsyncSession],
        decide: DecisionMaker | None = None,
        trading_day_tz: str = "UTC",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        step_timeout_s: float = STEP_TIMEOUT_S,
    ) -> None:
        self._profile = profile
        self._strategy_id = strategy_id
        self._market = market
        self._gate = gate
        self._risk = risk
        self._bus = bus
        self._sessions = sessions
        self._decide: DecisionMaker = decide or self._stub_decide
        self._tz = trading_day_tz
        self._clock = clock
        self._step_timeout = step_timeout_s
        self._admission_lock = asyncio.Lock()
        self._tasks: dict[int, asyncio.Task[None]] = {}
        self._checks: dict[int, asyncio.Future[OrderResult]] = {}  # cycles inside check()
        self._closing = False
        self._suspended = False

    @property
    def admission_suspended(self) -> bool:
        return self._suspended

    def _suspend(self, error: CommitUncertain) -> None:
        """`on_uncertain` callback: called by the bus-owned unit itself (D15)."""
        if not self._suspended:
            self._suspended = True
            log.error("cycle.admission_suspended", error=str(error))

    async def join(self) -> None:
        """Wait until every running cycle task has finished (tests; not a shutdown path)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks.values()), return_exceptions=True)
            await asyncio.sleep(0)  # let done-callbacks drop finished tasks

    # --- admission (G14) -------------------------------------------------------------

    async def on_candidate(self, event: CandidateSignal) -> None:
        """Bus handler. Never raises: a failed admission drops the candidate."""
        try:
            await self._admit(event)
        except Exception:
            log.exception("cycle.admission_failed", symbol=event.symbol, rule_id=event.rule_id)

    def _blocked(self) -> str | None:
        if self._closing:
            return "shutting down"
        if self._suspended:
            return "admission suspended: commit uncertain"
        return None

    @staticmethod
    def _drop(event: CandidateSignal, reason: str) -> None:
        log.info("cycle.dropped", symbol=event.symbol, rule_id=event.rule_id, reason=reason)

    async def _admit(self, event: CandidateSignal) -> None:
        if (reason := self._blocked()) is not None:
            self._drop(event, reason)
            return
        now = self._clock()
        if signal_is_stale(event.bar_ts, event.timeframe, now):
            self._drop(event, "signal stale")
            return
        try:
            status = await self._timed(self._gate.status(now))
        except Exception as e:  # an unanswered gate is not an allowed gate
            self._drop(event, f"gate unavailable: {type(e).__name__}")
            return
        if not status.entries_allowed:
            self._drop(event, f"entries blocked: {', '.join(status.reasons)}")
            return
        async with self._admission_lock:
            if (reason := self._blocked()) is not None:  # re-checked inside the lock
                self._drop(event, reason)
                return
            why: list[str] = []

            async def build(session: AsyncSession) -> DomainEvent | None:
                # Runs under the bus lock. A unit that held the lock before us may have
                # hit CommitUncertain and suspended admission while we waited (the bus
                # calls on_uncertain under the same lock), so check again here.
                if (blocked := self._blocked()) is not None:
                    why.append(blocked)
                    return None
                open_symbols = await repo.open_cycle_symbols(session)
                if len(open_symbols) >= self._profile.scanner.max_concurrent_cycles:
                    why.append("max_concurrent_cycles")
                    return None
                if event.symbol in open_symbols:
                    why.append("open cycle on symbol")
                    return None
                if any(o.symbol == event.symbol for o in await repo.open_entry_orders(session)):
                    why.append("open entry order on symbol")
                    return None
                row = await repo.insert_cycle(
                    session,
                    strategy_id=self._strategy_id,
                    symbol=event.symbol,
                    rule_id=event.rule_id,
                    signal_features=signal_features(event),
                    state=S.NEW,
                    now=now,
                )
                return CycleTransitioned(
                    ts=now,
                    cycle_id=row.id,
                    from_state=None,
                    to_state=S.NEW,
                    details={"rule_id": event.rule_id, "score": event.score},
                )

            published = await self._bus.publish_tx(build, _verify, on_uncertain=self._suspend)
            if published is None or published.cycle_id is None:
                self._drop(event, why[0] if why else "not admitted")
                return
            cycle_id = published.cycle_id
            if self._closing:  # committed after close() began: recover() ends it
                log.warning("cycle.not_started", cycle_id=cycle_id, reason="shutting down")
                return None
            task = asyncio.create_task(self._run(cycle_id, event), name=f"cycle-{cycle_id}")
            self._tasks[cycle_id] = task
            task.add_done_callback(functools.partial(self._task_done, cycle_id))
        log.info("cycle.admitted", cycle_id=cycle_id, symbol=event.symbol, rule_id=event.rule_id)
        return None

    def _task_done(self, cycle_id: int, task: asyncio.Task[None]) -> None:
        self._tasks.pop(cycle_id, None)
        if not task.cancelled() and (exc := task.exception()) is not None:
            log.error("cycle.task_crashed", cycle_id=cycle_id, error=repr(exc))

    # --- recovery --------------------------------------------------------------------

    async def recover(self) -> None:
        """Startup: end cycles that provably sent nothing; leave the rest to WP 1.5."""
        async with self._sessions() as session:
            rows = await repo.open_cycles(session)
            with_order = {r.id for r in rows if await repo.has_order_for_cycle(session, r.id)}
        for row in rows:
            state = CycleState(row.state)
            with bind_cycle(row.id):
                if state in BEFORE_CHECK or (state is S.RISK_CHECK and row.id not in with_order):
                    await self._move(row.id, state, S.ERROR, reason="interrupted")
                else:
                    log.warning("cycle.awaiting_reconciliation", state=state)

    # --- transitions -----------------------------------------------------------------

    async def _move(
        self,
        cycle_id: int,
        from_: CycleState,
        to: CycleState,
        details: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> None:
        check_edge(from_, to)  # ValueError: a bug, handled like any step error
        now = self._clock()
        event = CycleTransitioned(
            ts=now,
            cycle_id=cycle_id,
            from_state=from_,
            to_state=to,
            details={**(details or {}), **({"reason": reason} if reason else {})},
        )

        async def build(session: AsyncSession) -> DomainEvent:
            await repo.transition_cycle(
                session, cycle_id, from_state=from_, to_state=to, now=now, reason=reason
            )
            return event

        try:
            await self._bus.publish_tx(build, _verify, on_uncertain=self._suspend)
        except (CommitUncertain, asyncio.CancelledError):
            raise
        except Exception as e:
            raise _WriteFailed(f"{from_} -> {to}") from e
        log.info("cycle.transition", from_state=from_, to_state=to, reason=reason)

    async def _advance(
        self,
        cur: _Cursor,
        to: CycleState,
        details: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> None:
        await self._move(cur.cycle_id, cur.state, to, details, reason)
        cur.state = to

    # --- one cycle -------------------------------------------------------------------

    async def _run(self, cycle_id: int, candidate: CandidateSignal) -> None:
        with bind_cycle(cycle_id):
            cur = _Cursor(cycle_id, S.NEW)
            try:
                intent = await self._prepare(cur, candidate)
            except asyncio.CancelledError:
                raise
            except CommitUncertain:
                log.error("cycle.stuck", state=cur.state, reason="commit uncertain")
                return
            except Exception as e:
                if isinstance(e, _WriteFailed):
                    log.error("cycle.transition_failed", state=cur.state, error=repr(e.__cause__))
                    reason = "transition write failed"
                elif isinstance(e, TimeoutError):
                    log.error("cycle.step_timeout", state=cur.state)
                    reason = f"{cur.state}: timeout"
                else:
                    log.exception("cycle.step_failed", state=cur.state)
                    reason = f"{cur.state}: {type(e).__name__}"
                await self._fail(cur, reason)
                return
            if intent is not None:
                await self._risk_check(cur, intent)

    async def _fail(self, cur: _Cursor, reason: str) -> None:
        """General rule (§9): one attempt to write ERROR, else the row keeps its state."""
        try:
            await self._advance(cur, S.ERROR, reason=reason)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("cycle.stuck", state=cur.state, reason=reason)

    async def _timed(self, step: Awaitable[T]) -> T:
        async with asyncio.timeout(self._step_timeout):
            return await step

    async def _prepare(self, cur: _Cursor, candidate: CandidateSignal) -> EntryIntent | None:
        """NEW .. RISK_CHECK. Returns the intent, or None when the cycle already ended."""
        await self._advance(cur, S.ENRICHING)
        snap = await self._timed(self._enrich(candidate))
        if isinstance(snap, str):
            await self._advance(cur, S.EXPIRED, reason=snap)
            return None
        await self._advance(cur, S.ANALYZING, details=snap.details())
        await self._advance(cur, S.DECIDING, details=dict(NO_ANALYST))
        decision = await self._timed(self._decide(candidate, snap))
        if isinstance(decision, StubSkip):
            await self._advance(cur, S.REJECTED, reason=f"decision: skip ({decision.reason})")
            return None
        intent = EntryIntent(
            cycle_id=cur.cycle_id,
            symbol=candidate.symbol,
            stop=decision.stop,
            take_profit=decision.take_profit,
            confidence=decision.confidence,
            size_fraction=decision.size_fraction,
        )
        await self._advance(cur, S.RISK_CHECK, details=intent.model_dump(mode="json"))
        if signal_is_stale(candidate.bar_ts, candidate.timeframe, self._clock()):
            await self._advance(cur, S.EXPIRED, reason="signal stale")
            return None
        return intent

    async def _enrich(self, candidate: CandidateSignal) -> CycleSnapshot | str:
        """The snapshot, or the reason the cycle expires."""
        symbol = candidate.symbol
        if self._market.is_stale(symbol):
            return "symbol stale"
        quote = self._market.quote(symbol)
        ask = None if quote is None else _dec(quote.ask)
        if ask is None or ask <= 0:
            return "no ask"
        features = compute_features(
            self._market.history(symbol, self._profile.timeframes.signal),
            trading_day_tz=self._tz,
        )
        if not features["atr14"]:
            return "ATR missing"
        atr = _dec(features["atr14"][-1])
        if atr is None:
            return "ATR not finite"
        if atr <= 0:
            return "ATR not positive"
        return CycleSnapshot(
            ask=ask,
            bid=_dec(quote.bid) if quote is not None else None,
            last_price=_dec(self._market.last_price(symbol)),
            atr=atr,
        )

    async def _stub_decide(self, candidate: CandidateSignal, snap: CycleSnapshot) -> Decision:
        return stub.decide(candidate.score, snap.ask, snap.atr, self._profile.risk)

    async def _risk_check(self, cur: _Cursor, intent: EntryIntent) -> None:
        """RISK_CHECK onwards. `check()` once; after it, no fallback ERROR write (§9)."""
        check = asyncio.ensure_future(self._risk.check(intent))
        check.add_done_callback(_consume)
        self._checks[cur.cycle_id] = check
        try:
            try:
                result = await asyncio.shield(check)  # only close() may force it
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("cycle.check_failed")
                await self._after_check(cur, S.ERROR, reason=f"RISK_CHECK: {type(e).__name__}")
                return
            decision = result.decision
            if not decision.approved:
                reason = decision.reasons[0] if decision.reasons else "rejected"
                await self._after_check(cur, S.REJECTED, reason=reason)
                return
            submitted = {
                "order_row_id": result.order_row_id,
                "status": result.status,
                "broker_order_id": result.broker_order_id,
                "qty": None if decision.qty is None else str(decision.qty),
            }
            if not await self._after_check(cur, S.SUBMITTED, details=submitted):
                return
            if result.status == "filled":
                await self._after_check(cur, S.FILLED)
            elif result.status in ("rejected", "not_submitted"):
                await self._after_check(cur, S.REJECTED, reason=f"order {result.status}")
            else:  # new / unknown / canceled: WP 1.5 reconciles from the broker's fill qty
                log.info("cycle.awaiting_order", status=result.status)
        finally:
            self._checks.pop(cur.cycle_id, None)

    async def _after_check(
        self,
        cur: _Cursor,
        to: CycleState,
        details: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> bool:
        try:
            await self._advance(cur, to, details, reason)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:  # incl. CommitUncertain: the order row decides what happens
            log.exception("cycle.stuck", state=cur.state, to_state=to)
            return False
