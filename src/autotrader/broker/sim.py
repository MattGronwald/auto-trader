"""In-process broker for tests, backtests and dry runs (SPEC §4.6, §10).

Fill model, driven by `process_bar()`:
- Orders fill on the first bar processed after submission, never on the bar the
  submitter saw ("next bar open"). Per symbol, a bar is only processed if it is newer
  than the last one; duplicates and older bars (replays, backfill overlap) are ignored,
  so they can neither fill orders nor move the marked price. One timeframe per sim.
- Market: bar open +/- `slippage_bps`, taker fee.
- Limit: fills when the bar trades through the limit, at the better of open and limit,
  maker fee, no slippage.
- Fees are charged in quote currency (USD).
- Spot only: sells can never exceed the position (no shorting).

Cash is checked at submit (reserving pending buys) and again at fill, because the price
can gap; a buy that no longer fits is rejected at fill. Bar floats are converted via
`str()` so `0.1` becomes `Decimal("0.1")`, not its binary expansion.
"""

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import structlog

from autotrader.broker.base import (
    Account,
    AssetRules,
    BrokerOrder,
    FeeSchedule,
    Fill,
    Mode,
    OrderAck,
    OrderNotFound,
    OrderRejected,
    OrderState,
    OrderStatus,
    Position,
)
from autotrader.core.types import Bar

_BPS = Decimal(10_000)
log = structlog.get_logger(__name__)


def _dec(value: float) -> Decimal:
    return Decimal(str(value))


@dataclass
class _Order:
    id: str
    order: BrokerOrder
    submitted_at: datetime
    status: OrderState = "new"
    filled_qty: Decimal = Decimal(0)
    avg_fill_price: Decimal | None = None
    reason: str | None = None


@dataclass
class _Holding:
    qty: Decimal
    avg_entry_price: Decimal


class SimBroker:
    def __init__(
        self,
        *,
        cash: Decimal,
        fees: FeeSchedule,
        slippage_bps: Decimal,
        asset_rules: Mapping[str, AssetRules],
        mode: Mode = "paper",
        timeframe: str = "1m",
    ) -> None:
        self.cash = cash
        self.fees = fees
        self.slippage_bps = slippage_bps
        self.asset_rules_by_symbol = dict(asset_rules)
        self.broker_mode = mode
        self.timeframe = timeframe
        self._last_bar_ts: dict[str, datetime] = {}
        self._holdings: dict[str, _Holding] = {}
        self._orders: dict[str, _Order] = {}
        self._last_price: dict[str, Decimal] = {}
        self._now: datetime | None = None
        self._seq = 0
        self._fills: asyncio.Queue[Fill] = asyncio.Queue()

    # --- Broker protocol -------------------------------------------------------------

    async def get_account(self) -> Account:
        marked = sum((h.qty * self._last_price[s] for s, h in self._holdings.items()), Decimal(0))
        return Account(equity=self.cash + marked, cash=self.cash)

    async def get_positions(self) -> list[Position]:
        return [
            Position(symbol=s, qty=h.qty, avg_entry_price=h.avg_entry_price)
            for s, h in sorted(self._holdings.items())
        ]

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        self._validate(order)
        self._seq += 1
        oid = f"sim-{self._seq}"
        submitted_at = self._now or datetime.now(UTC)
        self._orders[oid] = _Order(oid, order, submitted_at)
        return OrderAck(
            order_id=oid,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            status="new",
            submitted_at=submitted_at,
        )

    async def cancel_order(self, order_id: str) -> None:
        o = self._get(order_id)
        if o.status != "new":
            raise OrderRejected(f"order {order_id} is not open ({o.status})")
        o.status = "canceled"

    async def get_order(self, order_id: str) -> OrderStatus:
        o = self._get(order_id)
        return OrderStatus(
            order_id=o.id,
            status=o.status,
            filled_qty=o.filled_qty,
            avg_fill_price=o.avg_fill_price,
            reason=o.reason,
        )

    async def close_position(self, symbol: str) -> OrderAck:
        if symbol not in self._holdings:
            raise OrderRejected(f"no position in {symbol}")
        qty = self._holdings[symbol].qty - self._pending_qty(symbol, "sell")
        if qty <= 0:
            raise OrderRejected(f"position in {symbol} is already fully reserved by pending sells")
        return await self.submit_order(
            BrokerOrder(symbol=symbol, side="sell", qty=qty, type="market")
        )

    async def close_all(self) -> list[OrderAck]:
        """Close every position; a rejected symbol is logged and skipped so it cannot
        stop the others from being flattened. Returns the acks of submitted closes."""
        acks: list[OrderAck] = []
        for symbol in sorted(self._holdings):
            try:
                acks.append(await self.close_position(symbol))
            except OrderRejected as e:
                log.warning("sim.close_skipped", symbol=symbol, reason=str(e))
        return acks

    async def stream_fills(self) -> AsyncIterator[Fill]:
        while True:
            yield await self._fills.get()

    def mode(self) -> Mode:
        return self.broker_mode

    def fee_schedule(self) -> FeeSchedule:
        return self.fees

    async def asset_rules(self, symbol: str) -> AssetRules:
        try:
            return self.asset_rules_by_symbol[symbol]
        except KeyError:
            raise OrderRejected(f"unknown symbol {symbol}") from None

    # --- simulation ------------------------------------------------------------------

    def process_bar(self, bar: Bar) -> bool:
        """Fill open orders for `bar.symbol` against this bar, then record its close.

        Returns False (and does nothing) for a bar that is not newer than the last one
        processed for its symbol.
        """
        if bar.timeframe != self.timeframe:
            raise ValueError(f"sim runs on {self.timeframe} bars, got timeframe {bar.timeframe}")
        last = self._last_bar_ts.get(bar.symbol)
        if last is not None and bar.ts <= last:
            return False
        self._last_bar_ts[bar.symbol] = bar.ts
        for o in [o for o in self._orders.values() if o.status == "new"]:
            if o.order.symbol == bar.symbol:
                self._try_fill(o, bar)
        self._last_price[bar.symbol] = _dec(bar.close)
        self._now = bar.ts
        return True

    def _try_fill(self, o: _Order, bar: Bar) -> None:
        order, open_ = o.order, _dec(bar.open)
        if order.type == "market":
            slip = self.slippage_bps / _BPS
            price = open_ * (1 + slip) if order.side == "buy" else open_ * (1 - slip)
            fee_bps = self.fees.taker_bps
        else:
            limit = cast(Decimal, order.limit_price)  # BrokerOrder guarantees it for limits
            if order.side == "buy" and _dec(bar.low) <= limit:
                price = min(open_, limit)
            elif order.side == "sell" and _dec(bar.high) >= limit:
                price = max(open_, limit)
            else:
                return
            fee_bps = self.fees.maker_bps
        notional = price * order.qty
        fee = notional * fee_bps / _BPS
        if order.side == "buy":
            if notional + fee > self.cash:
                o.status, o.reason = "rejected", "insufficient cash at fill"
                return
            self.cash -= notional + fee
            held = self._holdings.get(order.symbol)
            if held is None:
                self._holdings[order.symbol] = _Holding(order.qty, price)
            else:
                total = held.qty + order.qty
                held.avg_entry_price = (held.qty * held.avg_entry_price + notional) / total
                held.qty = total
        else:
            held = self._holdings[order.symbol]  # reserved at submit; cannot be missing
            self.cash += notional - fee
            held.qty -= order.qty
            if held.qty == 0:
                del self._holdings[order.symbol]
        o.status, o.filled_qty, o.avg_fill_price = "filled", order.qty, price
        self._fills.put_nowait(
            Fill(
                order_id=o.id,
                symbol=order.symbol,
                side=order.side,
                qty=order.qty,
                price=price,
                fee=fee,
                ts=bar.ts,
            )
        )

    def _validate(self, order: BrokerOrder) -> None:
        rules = self.asset_rules_by_symbol.get(order.symbol)
        if rules is None:
            raise OrderRejected(f"unknown symbol {order.symbol}")
        last = self._last_price.get(order.symbol)
        if last is None:
            raise OrderRejected(f"no price for {order.symbol} yet")
        if order.qty % rules.qty_increment != 0:
            raise OrderRejected(
                f"qty {order.qty} is not a multiple of increment {rules.qty_increment}"
            )
        if order.qty < rules.min_qty:
            raise OrderRejected(f"qty {order.qty} below minimum quantity {rules.min_qty}")
        reference = order.limit_price if order.limit_price is not None else last
        if order.qty * reference < rules.min_notional:
            raise OrderRejected(f"notional below minimum {rules.min_notional}")
        if order.side == "buy":
            available = self.cash - self._reserved_cash()
            if self._estimated_cost(order, last) > available:
                raise OrderRejected(f"insufficient cash: {available} available")
        else:
            held = self._holdings.get(order.symbol)
            free = (held.qty if held else Decimal(0)) - self._pending_qty(order.symbol, "sell")
            if order.qty > free:
                raise OrderRejected(f"sell qty {order.qty} exceeds position ({free} free)")

    def _estimated_cost(self, order: BrokerOrder, last: Decimal) -> Decimal:
        if order.limit_price is not None:
            return order.limit_price * order.qty * (1 + self.fees.maker_bps / _BPS)
        price = last * (1 + self.slippage_bps / _BPS)
        return price * order.qty * (1 + self.fees.taker_bps / _BPS)

    def _reserved_cash(self) -> Decimal:
        return sum(
            (
                self._estimated_cost(o.order, self._last_price[o.order.symbol])
                for o in self._orders.values()
                if o.status == "new" and o.order.side == "buy"
            ),
            Decimal(0),
        )

    def _pending_qty(self, symbol: str, side: str) -> Decimal:
        return sum(
            (
                o.order.qty
                for o in self._orders.values()
                if o.status == "new" and o.order.symbol == symbol and o.order.side == side
            ),
            Decimal(0),
        )

    def _get(self, order_id: str) -> _Order:
        try:
            return self._orders[order_id]
        except KeyError:
            raise OrderNotFound(order_id) from None
