"""Risk Engine inputs and outputs (SPEC §4.5, spec 2026-10-11). Frozen; exact Decimals."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from autotrader.broker.base import AssetRules, BrokerOrder, Dec, FeeSchedule, Fill


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class EntryIntent(_Frozen):
    """What the Decision Maker wants. No qty and no price: the engine sizes and prices."""

    cycle_id: int
    symbol: str
    stop: Dec = Field(gt=0)
    take_profit: Dec = Field(gt=0)
    confidence: Dec = Field(ge=0, le=1)
    size_fraction: Dec = Field(gt=0, le=1)


ExitReason = Literal["stop", "take_profit", "time", "trailing", "kill", "flatten", "stale"]


class ExitRequest(_Frozen):
    """Reduce-only, full exit (G1). `external`: a sell of a broker holding with no journal
    position (flatten / kill-flatten only)."""

    symbol: str
    reason: ExitReason
    cycle_id: int | None = None
    external: bool = False


class HeldPosition(_Frozen):
    symbol: str
    qty: Dec
    # qty x the feed's last price; None when the feed has no price or the symbol is stale,
    # and always None for exits, which read the broker only. Entries fail closed (check 8).
    market_value: Dec | None
    avg_entry_price: Dec  # cost basis: informational exposure estimate only, never a limit
    min_qty: Dec  # below it the position is dust: unsellable, ignored by the checks

    @property
    def is_dust(self) -> bool:
        return self.qty < self.min_qty


class OpenEntry(_Frozen):
    symbol: str
    notional: Dec


class PortfolioState(_Frozen):
    equity: Dec
    cash: Dec
    day_start_equity: Dec
    hwm: Dec
    positions: tuple[HeldPosition, ...] = ()
    open_entries: tuple[OpenEntry, ...] = ()
    entries_last_hour: int = 0
    entries_today: int = 0
    strategy_trades_n: int = 0
    strategy_win_rate: Dec | None = None


class MarketSnapshot(_Frozen):
    last_trade: Dec | None
    bid: Dec | None
    ask: Dec | None
    atr: Dec | None
    stale: bool
    rules: AssetRules
    fees: FeeSchedule


Pause = Literal["until_next_day", "manual"]


class RiskDecision(_Frozen):
    approved: bool
    reasons: tuple[str, ...]
    adjusted: dict[str, str] = Field(default_factory=dict)
    qty: Dec | None = None
    order: BrokerOrder | None = None
    pause: Pause | None = None  # side effect the engine must apply on rejection


# Order statuses after which nothing more fills (broker finals plus the local
# "not_submitted"). A final row settles once its fills are all booked (spec 4.3); until
# then it blocks its symbol but reserves no notional (spec 4.7).
FINAL = frozenset({"filled", "canceled", "rejected", "not_submitted"})


class OrderUpdate(_Frozen):
    """One order's broker view for the ledger (WP 1.5b, spec section 4.2). `status` and
    `fills` are two separate broker reads, so the fills may be ahead of `filled_qty`."""

    order_row_id: int
    status: str  # a broker OrderState, or the local finals "not_submitted" / "rejected"
    filled_qty: Dec  # the broker's cumulative filled qty (0 for local finals)
    fills: tuple[Fill, ...] = ()
    min_qty: Dec | None = None  # None only for local finals: no broker call, no fills


class SyncResult(_Frozen):
    """One sync pass. `failed` lists order rows whose broker state could not be read."""

    updates: tuple[OrderUpdate, ...] = ()
    failed: tuple[int, ...] = ()

    @property
    def complete(self) -> bool:
        return not self.failed


class CancelResult(_Frozen):
    """One `cancel_open_entries` pass. `requested`: rows whose cancel the broker accepted;
    `failed`: rows whose cancel raised; `unresolved`: non-final buys with no broker id."""

    requested: tuple[int, ...] = ()
    failed: tuple[int, ...] = ()
    unresolved: tuple[int, ...] = ()


class SyncIncomplete(RuntimeError):
    """An unsettled order's broker state could not be read: entries fail closed (PLAN 3.2
    item 4) until it is resolved."""
