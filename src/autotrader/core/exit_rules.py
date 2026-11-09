"""Pure exit rules (spec 2026-10-19 sections 4.3.1 and 4.4; D18 decisions 1-4 and 7).

No I/O, no DB, no broker: the Position Manager feeds in a stored `ExitState`, the latest
trusted bid and its age, and gets back the exit reason (if a rule fires) and the trailing
state to persist. Rules, in order: stop (or trailing stop, whichever is higher), take
profit, time limit, stale price. A step on which a rule fires never updates the trailing
state: the trigger is judged against the values stored before this price.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Protocol

from autotrader.core.config import Trailing
from autotrader.core.types import Bar
from autotrader.risk.types import FINAL, ExitReason

_BACKOFF_CAP_S = 30
_ALERT_FIRST = 3
_ALERT_EVERY = 10


@dataclass(frozen=True)
class ExitState:
    entry_price: Decimal
    stop: Decimal | None
    tp: Decimal | None
    time_limit_at: datetime | None
    atr_at_entry: Decimal | None
    trail_high: Decimal | None
    trail_stop: Decimal | None


@dataclass(frozen=True)
class TrailUpdate:
    trail_high: Decimal
    trail_stop: Decimal


@dataclass(frozen=True)
class ExitRules:
    trailing: Trailing
    stale_exit_enabled: bool
    stale_exit_s: float


def _stop_reason(state: ExitState, price: Decimal) -> ExitReason | None:
    """`price` at or below the effective stop; "trailing" only when it is the higher one."""
    stops = [s for s in (state.stop, state.trail_stop) if s is not None]
    if not stops or price > max(stops):
        return None
    trailing = state.trail_stop is not None and (
        state.stop is None or state.trail_stop > state.stop
    )
    return "trailing" if trailing else "stop"


def evaluate(
    state: ExitState,
    bid: Decimal | None,
    price_age_s: float | None,
    now: datetime,
    rules: ExitRules,
) -> tuple[ExitReason | None, TrailUpdate | None]:
    """`bid` None: no trusted price. `price_age_s` None: unknown age (never stale)."""
    reason: ExitReason | None = None
    if bid is not None:
        reason = _stop_reason(state, bid)
        if reason is None and state.tp is not None and bid >= state.tp:
            reason = "take_profit"
    if reason is None and state.time_limit_at is not None and now >= state.time_limit_at:
        reason = "time"
    if (
        reason is None
        and rules.stale_exit_enabled
        and price_age_s is not None
        and price_age_s > rules.stale_exit_s
    ):
        reason = "stale"
    if reason is not None or bid is None:
        return reason, None
    return None, _trail(state, bid, rules.trailing)


def _trail(state: ExitState, bid: Decimal, cfg: Trailing) -> TrailUpdate | None:
    if not cfg.enabled or state.atr_at_entry is None or state.stop is None:
        return None
    r = state.entry_price - state.stop
    if r <= 0:
        return None
    if state.trail_high is None and bid < state.entry_price + cfg.trigger_r * r:
        return None
    high = bid if state.trail_high is None else max(state.trail_high, bid)
    floor = [state.stop, high - cfg.trail_atr * state.atr_at_entry]
    stop = max([*floor, state.trail_stop] if state.trail_stop is not None else floor)
    if (high, stop) == (state.trail_high, state.trail_stop):
        return None
    return TrailUpdate(trail_high=high, trail_stop=stop)


def evaluate_bar(state: ExitState, bar: Bar) -> ExitReason | None:
    """Bar-based callers only: a bar through both stop and TP counts as the stop."""
    reason = _stop_reason(state, Decimal(str(bar.low)))
    if reason is None and state.tp is not None and Decimal(str(bar.high)) >= state.tp:
        return "take_profit"
    return reason


SellState = Literal["attempt", "unresolved", "in_flight", "settling"]
_RANK: dict[SellState, int] = {"attempt": 0, "settling": 1, "in_flight": 2, "unresolved": 3}


class SellRow(Protocol):
    @property
    def broker_order_id(self) -> str | None: ...

    @property
    def status(self) -> str: ...


def _classify_row(row: SellRow) -> SellState:
    if row.broker_order_id is None:
        return "attempt" if row.status in ("not_submitted", "rejected") else "unresolved"
    if row.status not in FINAL:
        return "in_flight"
    return "attempt" if row.status == "rejected" else "settling"


def classify_sells(rows: Sequence[SellRow]) -> SellState:
    """Highest of the rows by precedence unresolved > in_flight > settling > attempt."""
    states = [_classify_row(r) for r in rows]
    return max(states, key=_RANK.__getitem__, default="attempt")


@dataclass
class Backoff:
    """Retry delay 1, 2, 4, ... capped at 30 s; alert at the 3rd failure, then every 10th."""

    failures: int = 0
    next_at: datetime | None = None

    def due(self, now: datetime) -> bool:
        return self.next_at is None or now >= self.next_at

    def fail(self, now: datetime) -> bool:
        self.failures += 1
        delay = min(2 ** (self.failures - 1), _BACKOFF_CAP_S)
        self.next_at = now + timedelta(seconds=delay)
        return self.failures == _ALERT_FIRST or (
            self.failures > _ALERT_FIRST and (self.failures - _ALERT_FIRST) % _ALERT_EVERY == 0
        )

    def reset(self) -> None:
        self.failures = 0
        self.next_at = None
