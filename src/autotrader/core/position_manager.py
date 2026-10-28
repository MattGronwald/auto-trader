"""Position Manager (WP 1.5c; spec 2026-10-19 sections 4.3-4.4; D18 decisions 1-5, 7, 10-12).

`step()` runs every second in `run core`'s supervised `positions.manage` loop. Per open
journal position, each in its own `try` (a failure is logged `positions.step_failed` and
never stops the others): write `time_limit_at` once, evaluate the pure exit rules
(`core/exit_rules.py`) against the trusted bid, persist a trailing update, latch a fired
trigger in one `publish_tx` unit with its `ExitTriggered` event, then (when the broker can
place orders) work the exit path: judge settled sells, classify the unsettled ones, and call
`risk.exit()` when the latest state allows an attempt and the backoff is due. The manager
never holds a broker: it talks to the Risk Engine only.

Durable (journal): the latch (`exit_trigger`, `exit_triggered_at`), `time_limit_at` and the
trailing values. A latched trigger is never re-evaluated and a restart honours it.

In memory (per process, forgotten on restart): the retry backoff, the pending sell rows
whose outcome is still to be judged (clarification 5: each settled row counts once by id;
an accepted order never resets the backoff, a settled fill does), the dust hold-back and
the alert throttles. A restart therefore re-attempts a due exit at once (D7).

Kill handling and flatten (`_control_phase`, `_flat_check`) come with Task 10.
"""

import math
import typing
from collections.abc import Callable, Collection, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol, cast

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.bus import CommitUncertain, EventBus
from autotrader.core.config import AppConfig, StrategyProfile
from autotrader.core.exit_rules import (
    Backoff,
    ExitRules,
    ExitState,
    classify_sells,
    evaluate,
)
from autotrader.core.kill import ControlStatus
from autotrader.core.ledger import LedgerEvent, verify_unit
from autotrader.core.logging import bind_cycle
from autotrader.core.types import Quote
from autotrader.journal import repo
from autotrader.journal.models import Order, Position
from autotrader.risk.engine import OrderResult
from autotrader.risk.types import FINAL, CancelResult, ExitReason, ExitRequest, HeldPosition

log = structlog.get_logger(__name__)

Key = int | tuple[str, str]  # a position id, or ("ext", symbol) for external sales (Task 10)
ALERT_EVERY = timedelta(seconds=60)
STUCK_AFTER = timedelta(seconds=60)
_REASONS = frozenset(typing.get_args(ExitReason))


class ExitTriggered(LedgerEvent):
    position_id: int
    symbol: str
    reason: str
    bid: Decimal | None


class ExitEngine(Protocol):
    async def exit(self, request: ExitRequest) -> OrderResult: ...
    async def cancel_open_entries(self, exclude: Collection[int] = ()) -> CancelResult: ...
    async def sellable_positions(self) -> tuple[HeldPosition, ...]: ...
    def can_place_orders(self) -> bool: ...


class ExitMarketView(Protocol):
    def quote(self, symbol: str) -> Quote | None: ...
    def is_stale(self, symbol: str) -> bool: ...
    def price_age_s(self, symbol: str) -> float | None: ...


class ControlSource(Protocol):
    @property
    def latest(self) -> ControlStatus | None: ...


def _as_reason(value: str) -> ExitReason:
    if value not in _REASONS:
        raise ValueError(f"unknown exit trigger {value!r}")
    return cast(ExitReason, value)


def _key_fields(key: Key) -> dict[str, Any]:
    if isinstance(key, int):
        return {"position_id": key, "external": False}
    return {"symbol": key[1], "external": True}


class PositionManager:
    def __init__(
        self,
        risk: ExitEngine,
        market: ExitMarketView,
        sessions: async_sessionmaker[AsyncSession],
        bus: EventBus,
        controls: ControlSource,
        *,
        profile: StrategyProfile,
        config: AppConfig,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._risk = risk
        self._market = market
        self._sessions = sessions
        self._bus = bus
        self._controls = controls
        self._profile = profile
        self._config = config
        self._clock = clock
        self._rules = ExitRules(
            profile.risk.trailing, config.feed_stale_exit_enabled, config.feed.stale_exit_s
        )
        self._started = clock()  # cold-start grace for never-priced symbols
        self._backoff: dict[Key, Backoff] = {}
        self._pending: dict[Key, set[int]] = {}
        self._dust: dict[int, tuple[Decimal, Decimal]] = {}  # position id -> (held, min_qty)
        self._last_unresolved_log: dict[Key, datetime] = {}
        self._last_stuck_log: dict[Key, datetime] = {}
        self._unprotected: set[int] = set()
        self._stale_held: set[str] = set()
        self._cannot_trade_logged = False

    async def step(self) -> None:
        now = self._clock()
        can_trade = self._risk.can_place_orders()
        if not can_trade and not self._cannot_trade_logged:
            log.warning("positions.broker_cannot_trade")
            self._cannot_trade_logged = True
        await self._control_phase(now, can_trade)
        async with self._sessions() as session:
            positions = await repo.open_positions(session)
            unsettled = await repo.unsettled_orders(session)
        for pos in positions:
            with bind_cycle(pos.cycle_id):
                try:
                    await self._manage(pos, unsettled, now, can_trade)
                except Exception:
                    log.exception("positions.step_failed", position_id=pos.id, symbol=pos.symbol)
        self._alert_stale_held({p.symbol for p in positions})
        self._forget({p.id for p in positions})

    async def _control_phase(self, now: datetime, can_trade: bool) -> None:
        """Kill handling and flatten passes (Task 10)."""
        return

    async def _flat_check(self, now: datetime) -> None:
        """The flatten completion check (Task 10)."""
        return

    # --- per position ----------------------------------------------------------------

    async def _manage(
        self, pos: Position, unsettled: Sequence[Order], now: datetime, can_trade: bool
    ) -> None:
        time_limit = pos.time_limit_at
        if time_limit is None:
            time_limit = pos.opened_at + timedelta(minutes=self._profile.risk.max_hold_min)
            async with self._sessions() as session:
                await repo.set_time_limit(session, pos.id, time_limit)
        if pos.stop is None and pos.id not in self._unprotected:
            log.warning("positions.unprotected", position_id=pos.id, symbol=pos.symbol)
            self._unprotected.add(pos.id)
        trigger = pos.exit_trigger
        if trigger is None:
            trigger = await self._evaluate(pos, time_limit, now)
            if trigger is None:
                return
        if can_trade:
            sells = [o for o in unsettled if o.symbol == pos.symbol and o.side == "sell"]
            await self._exit_path(pos, trigger, sells, now)

    async def _evaluate(self, pos: Position, time_limit: datetime, now: datetime) -> str | None:
        """The fired and latched reason, or None (nothing fired, or the latch failed)."""
        state = ExitState(
            entry_price=pos.entry_price,
            stop=pos.stop,
            tp=pos.tp,
            time_limit_at=time_limit,
            atr_at_entry=pos.atr_at_entry,
            trail_high=pos.trail_high,
            trail_stop=pos.trail_stop,
        )
        bid = self._trusted_bid(pos.symbol, now)
        reason, trail = evaluate(state, bid, self._price_age(pos.symbol, now), now, self._rules)
        if trail is not None:
            async with self._sessions() as session:
                await repo.update_trailing(
                    session, pos.id, trail_high=trail.trail_high, trail_stop=trail.trail_stop
                )
        if reason is None or not await self._latch(pos, reason, bid, now):
            return None
        return reason

    def _trusted_bid(self, symbol: str, now: datetime) -> Decimal | None:
        quote = self._market.quote(symbol)
        if (
            quote is None
            or self._market.is_stale(symbol)
            or (now - quote.ts).total_seconds() > self._config.feed.stale_alert_s
        ):
            return None
        return Decimal(str(quote.bid))

    def _price_age(self, symbol: str, now: datetime) -> float | None:
        age = self._market.price_age_s(symbol)
        if age is None and (now - self._started).total_seconds() > self._config.feed.stale_exit_s:
            return math.inf
        return age

    async def _latch(
        self, pos: Position, reason: ExitReason, bid: Decimal | None, now: datetime
    ) -> bool:
        async def build(session: AsyncSession) -> ExitTriggered | None:
            if not await repo.latch_exit(session, pos.id, reason=reason, at=now):
                return None
            return ExitTriggered(
                ts=now,
                cycle_id=pos.cycle_id,
                position_id=pos.id,
                symbol=pos.symbol,
                reason=reason,
                bid=bid,
            )

        try:
            event = await self._bus.publish_tx(build, verify_unit)
        except CommitUncertain:
            log.error("positions.latch_uncertain", position_id=pos.id, symbol=pos.symbol)
            return False
        if event is None:  # already latched or closed elsewhere: skip this step
            return False
        log.warning(
            "positions.exit_triggered",
            position_id=pos.id,
            symbol=pos.symbol,
            reason=reason,
            bid=None if bid is None else str(bid),
        )
        return True

    # --- exit path -------------------------------------------------------------------

    async def _exit_path(
        self, pos: Position, trigger: str, sells: Sequence[Order], now: datetime
    ) -> None:
        backoff = self._backoff.setdefault(pos.id, Backoff())
        await self._judge_outcomes(pos.id, backoff, now)
        state = classify_sells(sells)
        if state in ("unresolved", "in_flight"):
            self._alert_unsettled(pos.id, sells, now, external=False)
            return
        if state == "settling":
            return
        if pos.qty == 0:  # all sold; waits for its other orders to settle (clarification 6)
            return
        hold = self._dust.get(pos.id)
        if hold is not None:
            held_qty, min_qty = hold
            if not (pos.qty > held_qty and pos.qty >= min_qty):
                return
            del self._dust[pos.id]
        if not backoff.due(now):
            return
        result = await self._risk.exit(
            ExitRequest(symbol=pos.symbol, reason=_as_reason(trigger), cycle_id=pos.cycle_id)
        )
        if result.dust and not (result.min_qty is not None and pos.qty >= result.min_qty):
            self._dust[pos.id] = (pos.qty, result.min_qty or pos.qty)
            log.warning(
                "positions.exit_dust", position_id=pos.id, symbol=pos.symbol, qty=str(pos.qty)
            )
            return
        if not result.dust:
            if result.broker_order_id is not None and result.status not in (
                "rejected",
                "not_submitted",
            ):
                if result.order_row_id is not None:  # accepted: judged once it settles
                    self._pending.setdefault(pos.id, set()).add(result.order_row_id)
                return
            if result.status in ("pending", "unknown"):
                return  # the next step classifies the row
        if backoff.fail(now):
            log.error(
                "positions.exit_failing",
                position_id=pos.id,
                symbol=pos.symbol,
                failures=backoff.failures,
                reasons=list(result.decision.reasons),
                status=result.status,
            )

    async def _judge_outcomes(self, key: Key, backoff: Backoff, now: datetime) -> None:
        """Judge each settled pending sell once by its row id (clarification 5): a zero fill
        counts one failure, any fill resets the backoff; unsettled rows stay pending."""
        pending = self._pending.get(key)
        if not pending:
            return
        async with self._sessions() as session:
            rows = await repo.orders_by_id(session, pending)
        for row in rows:
            if row.settled_at is None:
                continue
            pending.discard(row.id)
            if row.filled_qty > 0:
                backoff.reset()
            elif backoff.fail(now):
                log.error(
                    "positions.exit_failing",
                    order_row=row.id,
                    status=row.status,
                    failures=backoff.failures,
                    **_key_fields(key),
                )

    def _alert_unsettled(
        self, key: Key, sells: Sequence[Order], now: datetime, *, external: bool
    ) -> None:
        unresolved = [
            r.id
            for r in sells
            if r.broker_order_id is None and r.status not in ("not_submitted", "rejected")
        ]
        if unresolved and self._due(self._last_unresolved_log, key, now):
            log.error(
                "positions.exit_unresolved", key=str(key), order_rows=unresolved, external=external
            )
        in_flight = [r for r in sells if r.broker_order_id is not None and r.status not in FINAL]
        if (
            in_flight
            and now - min(r.submitted_at for r in in_flight) > STUCK_AFTER
            and self._due(self._last_stuck_log, key, now)
        ):
            log.error(
                "positions.exit_stuck",
                key=str(key),
                order_rows=[r.id for r in in_flight],
                external=external,
            )

    @staticmethod
    def _due(last: dict[Key, datetime], key: Key, now: datetime) -> bool:
        previous = last.get(key)
        if previous is not None and now - previous < ALERT_EVERY:
            return False
        last[key] = now
        return True

    # --- housekeeping ----------------------------------------------------------------

    def _alert_stale_held(self, symbols: set[str]) -> None:
        for symbol in sorted(symbols):
            age = self._market.price_age_s(symbol)
            stale = self._market.is_stale(symbol) or (
                age is not None and age > self._config.feed.stale_alert_s
            )
            if stale and symbol not in self._stale_held:
                log.warning("positions.stale_held", symbol=symbol, price_age_s=age)
                self._stale_held.add(symbol)
            elif not stale:
                self._stale_held.discard(symbol)
        self._stale_held &= symbols

    def _forget(self, open_ids: set[int]) -> None:
        """Drop the in-memory state of positions that are no longer open."""
        for state in (
            self._backoff,
            self._pending,
            self._dust,
            self._last_unresolved_log,
            self._last_stuck_log,
        ):
            for key in [k for k in state if isinstance(k, int) and k not in open_ids]:
                del state[key]
