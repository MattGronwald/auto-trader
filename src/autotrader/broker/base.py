"""Broker interface and domain types (SPEC §4.6).

Only the Risk Engine holds a `Broker` (SPEC §0.1). All money and quantities are
`Decimal`. Orders are `market` or `limit`: Alpaca crypto most likely has no bracket/OCO
orders (PLAN G2, verified in WP 0.7), so stops are enforced client-side by the Position
Manager.
"""

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Annotated, Literal, Protocol, Self

from pydantic import AwareDatetime, BaseModel, BeforeValidator, ConfigDict, Field, model_validator

Side = Literal["buy", "sell"]
OrderType = Literal["market", "limit"]
OrderState = Literal["new", "filled", "canceled", "rejected"]
Mode = Literal["paper", "live"]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


def _exact(value: object) -> object:
    """Money/qty boundary: a float has already lost precision (0.1 + 0.2), so refuse it.

    Decimal, int and numeric strings (JSON) are exact and accepted.
    """
    if isinstance(value, float | bool):
        raise ValueError(f"expected an exact Decimal, int or numeric string, got {value!r}")
    return value


Dec = Annotated[Decimal, BeforeValidator(_exact)]


class BrokerOrder(_Frozen):
    symbol: str
    side: Side
    qty: Dec = Field(gt=0)
    type: OrderType
    limit_price: Dec | None = Field(default=None, gt=0)
    client_order_id: str | None = None

    @model_validator(mode="after")
    def _limit_price_matches_type(self) -> Self:
        if (self.type == "limit") != (self.limit_price is not None):
            raise ValueError("limit_price is required for limit orders and only for them")
        return self


class OrderAck(_Frozen):
    order_id: str
    client_order_id: str | None
    symbol: str
    status: OrderState
    submitted_at: AwareDatetime


class OrderStatus(_Frozen):
    order_id: str
    status: OrderState
    filled_qty: Dec
    avg_fill_price: Dec | None
    reason: str | None = None  # set for rejections


class Fill(_Frozen):
    order_id: str
    symbol: str
    side: Side
    qty: Dec
    price: Dec
    fee: Dec  # in quote currency (USD); G10: Alpaca's real mechanics verified in 0.7
    ts: AwareDatetime


class Position(_Frozen):
    symbol: str
    qty: Dec
    avg_entry_price: Dec


class Account(_Frozen):
    equity: Dec  # cash + positions marked to last price
    cash: Dec


class FeeSchedule(_Frozen):
    maker_bps: Dec = Field(ge=0)
    taker_bps: Dec = Field(ge=0)


class AssetRules(_Frozen):
    """Per-symbol order constraints used by sizing (G4)."""

    min_qty: Dec = Field(gt=0)
    qty_increment: Dec = Field(gt=0)
    min_notional: Dec = Field(ge=0)


class BrokerError(Exception):
    pass


class OrderRejected(BrokerError):
    pass


class OrderNotFound(BrokerError):
    pass


class Broker(Protocol):
    async def get_account(self) -> Account: ...
    async def get_positions(self) -> list[Position]: ...
    async def submit_order(self, order: BrokerOrder) -> OrderAck: ...
    async def cancel_order(self, order_id: str) -> None: ...
    async def get_order(self, order_id: str) -> OrderStatus: ...
    async def close_position(self, symbol: str) -> OrderAck: ...
    async def close_all(self) -> list[OrderAck]: ...
    def stream_fills(self) -> AsyncIterator[Fill]: ...
    def mode(self) -> Mode: ...
    def fee_schedule(self) -> FeeSchedule: ...
    async def asset_rules(self, symbol: str) -> AssetRules: ...
