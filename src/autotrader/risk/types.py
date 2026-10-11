"""Risk Engine inputs and outputs (SPEC §4.5, spec 2026-10-11). Frozen; exact Decimals."""

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from autotrader.broker.base import AssetRules, BrokerOrder, Dec, FeeSchedule


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
    """Reduce-only, full exit (G1)."""

    symbol: str
    reason: ExitReason
    cycle_id: int | None = None


class HeldPosition(_Frozen):
    symbol: str
    qty: Dec
    market_value: Dec
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
    spread_bps: Dec | None
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


ZERO = Decimal(0)
