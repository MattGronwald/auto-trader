"""Broker interface and domain types (SPEC §4.6).

Only the Risk Engine holds a `Broker` (SPEC §0.1). All money and quantities are
`Decimal`. Orders are `market` or `limit`: Alpaca crypto has no bracket/OCO orders
(PLAN G2, confirmed 2026-10-11, docs/alpaca-notes.md), so stops are enforced
client-side by the Position Manager.
"""

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Annotated, Literal, Protocol, Self

from pydantic import AwareDatetime, BaseModel, BeforeValidator, ConfigDict, Field, model_validator

Side = Literal["buy", "sell"]
OrderType = Literal["market", "limit"]
TimeInForce = Literal["gtc", "ioc"]
# "partially_filled": some qty filled, the rest still open (not final). A partly filled
# order can end canceled or rejected with filled_qty > 0 (PLAN 3.2 item 3).
OrderState = Literal["new", "partially_filled", "filled", "canceled", "rejected"]
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
    time_in_force: TimeInForce = "gtc"
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
    """One broker execution. `fill_id` is unique and stable per execution, so a consumer
    applies each fill once (WP 1.5). `qty` is the gross qty traded at `price`; `fee` is
    denominated in `fee_asset`: a buy with `FeeSchedule.buy_fee_asset == "base"` pays
    it in the coin (net coins received = qty - fee), every sell and every "quote" buy
    pays it in USD (G10, D10, docs/alpaca-notes.md)."""

    fill_id: str
    order_id: str
    symbol: str
    side: Side
    qty: Dec
    price: Dec
    fee: Dec
    fee_asset: Literal["base", "quote"]
    ts: AwareDatetime


class Position(_Frozen):
    symbol: str
    qty: Dec
    avg_entry_price: Dec


class Account(_Frozen):
    equity: Dec  # cash + positions marked to last price
    cash: Dec


class FeeSchedule(_Frozen):
    maker_bps: Dec = Field(ge=0, lt=10_000)
    taker_bps: Dec = Field(ge=0, lt=10_000)  # < 100 %: D16 net stop loss stays positive
    # Which asset a buy's fee is taken from (G10, D10). "base": Alpaca deducts it from the
    # coin received, so the position is smaller than the fill. "quote": charged in USD on
    # top of the fill value. SimBroker models both (WP 1.R2). Sells always pay in the quote
    # currency. Sizing and expectancy (D16) use the matching cash-flow equation.
    buy_fee_asset: Literal["base", "quote"]


class AssetRules(_Frozen):
    """Per-symbol order constraints used by sizing and entry pricing (G4, D14)."""

    min_qty: Dec = Field(gt=0)
    qty_increment: Dec = Field(gt=0)
    min_notional: Dec = Field(ge=0)
    price_increment: Dec = Field(gt=0)


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
