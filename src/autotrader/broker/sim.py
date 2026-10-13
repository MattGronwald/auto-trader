"""In-process broker for tests, backtests and dry runs (SPEC §4.6, §10).

Fill model, driven by `process_bar()`:
- Orders fill on the first bar processed after submission, never on the bar the
  submitter saw ("next bar open"). Per symbol, a bar is only processed if it is newer
  than the last one; duplicates and older bars (replays, backfill overlap) are ignored,
  so they can neither fill orders nor move the marked price. One timeframe per sim.
- Across symbols, an order is stamped with the latest bar time seen for any symbol and
  only fills on a bar strictly later than that, so a lagging symbol's older bar can
  never fill an order placed after later information was observed.
- Market: bar open +/- `slippage_bps`, taker fee.
- Limit: fills when the bar trades through the limit, at the better of open and limit,
  maker fee, no slippage.
- IOC limit: evaluated on the next processed bar only; fills like a limit but pays the
  taker fee (it crosses the book); any unfilled remainder is canceled.
- Every execution is a slice of its order and emits one `Fill` (`fill_id` =
  "{order_id}:{n}"). By default an order fills completely in one slice; a test can queue
  a `FillPlan` (`script()`) to split it over bars, cancel or reject it (spec 2026-10-12
  section 5). Partly filled orders are `partially_filled` (not final); reservations cover
  only the unfilled remainder. `redeliver()` re-emits a fill for duplicate-delivery tests.
- Fees: sells pay in USD out of the proceeds. Buys follow `FeeSchedule.buy_fee_asset`:
  "quote" pays the fee in USD on top of the notional; "base" (Alpaca) pays exactly the
  notional and takes the fee from the coin, rounded up to `coin_precision` (1e-9). A
  slice too small for its rounded coin fee is rejected. `avg_entry_price` is the fill
  price (Alpaca's convention); cash is exact, never rounded to cents (spec section 3).
- Spot only: sells can never exceed the position (no shorting).
- `close_position` sells the exact held qty minus pending sell remainders, even when it
  is not a multiple of `qty_increment` (Alpaca accepted the exact net qty on paper); a
  remainder below `min_qty` is dust and is rejected (spec section 4). Whether Alpaca rejects
  a sell below `min_order_size` is unverified (WP 1.6).

Cash is checked at submit (reserving pending buys) and again at fill, because the price
can gap; a buy that no longer fits is rejected at fill. Bar floats are converted via
`str()` so `0.1` becomes `Decimal("0.1")`, not its binary expansion.
"""

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Literal, cast

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
    Side,
)
from autotrader.core.types import Bar

_BPS = Decimal(10_000)
_D0 = Decimal(0)
log = structlog.get_logger(__name__)


def _dec(value: float) -> Decimal:
    return Decimal(str(value))


def _floor(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _ceil(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


FillStep = Decimal | Literal["cancel", "reject"]


@dataclass(frozen=True)
class FillPlan:
    """Scripted outcome for one order (spec section 5): one step per bar on which the order
    is eligible and its price condition holds. A fraction `0 < f <= 1` fills that share of
    the *original* qty (rounded down, capped at the remainder); "cancel" / "reject" end
    the order, keeping earlier fills. When steps run out the order fills its remainder."""

    steps: tuple[FillStep, ...]

    def __post_init__(self) -> None:
        for step in self.steps:
            if isinstance(step, Decimal):
                # is_finite first: comparing a NaN Decimal raises InvalidOperation.
                if not step.is_finite() or not _D0 < step <= 1:
                    raise ValueError(f"fill step fraction must be in (0, 1], got {step}")
            elif step not in ("cancel", "reject"):
                raise ValueError(f"unknown fill step {step!r}")


@dataclass
class _Order:
    id: str
    order: BrokerOrder
    submitted_at: datetime
    status: OrderState = "new"
    filled_qty: Decimal = Decimal(0)
    filled_notional: Decimal = Decimal(0)
    fills: int = 0
    reason: str | None = None
    steps: deque[FillStep] = field(default_factory=deque)
    full_close: bool = False  # close_position: may be off-increment (spec section 4)

    @property
    def remaining(self) -> Decimal:
        return self.order.qty - self.filled_qty

    @property
    def is_open(self) -> bool:
        return self.status in ("new", "partially_filled")

    @property
    def avg_fill_price(self) -> Decimal | None:
        return None if self.filled_qty == 0 else self.filled_notional / self.filled_qty


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
        coin_precision: Decimal = Decimal("1e-9"),
    ) -> None:
        # Validate before any arithmetic: a negative quantum would turn the round-up into a
        # round-down, and 0 or NaN raise InvalidOperation in the modulo below.
        if (
            not isinstance(coin_precision, Decimal)
            or not coin_precision.is_finite()
            or coin_precision <= 0
        ):
            raise ValueError(f"coin_precision must be a finite Decimal > 0, got {coin_precision!r}")
        for symbol, symbol_rules in asset_rules.items():
            # Applies to both fee assets: quantities never go below one coin quantum, and a
            # coin fee rounded up to coin_precision can't exceed a minimal slice (spec section 3).
            if symbol_rules.qty_increment % coin_precision != 0:
                raise ValueError(
                    f"{symbol}: qty_increment {symbol_rules.qty_increment} is not a multiple "
                    f"of coin_precision {coin_precision}"
                )
        self.cash = cash
        self.fees = fees
        self.slippage_bps = slippage_bps
        self.asset_rules_by_symbol = dict(asset_rules)
        self.broker_mode = mode
        self.timeframe = timeframe
        self.coin_precision = coin_precision
        self._last_bar_ts: dict[str, datetime] = {}
        self._holdings: dict[str, _Holding] = {}
        self._orders: dict[str, _Order] = {}
        self._last_price: dict[str, Decimal] = {}
        self._now: datetime | None = None
        self._seq = 0
        self._fills: asyncio.Queue[Fill] = asyncio.Queue()
        self._fill_log: dict[str, Fill] = {}  # every emitted fill by id, for redeliver()
        self._scripts: dict[tuple[str, str], deque[FillPlan]] = {}

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
        return await self._submit(order, full_close=False)

    async def _submit(self, order: BrokerOrder, *, full_close: bool) -> OrderAck:
        self._validate(order, full_close=full_close)
        self._seq += 1
        oid = f"sim-{self._seq}"
        submitted_at = self._now or datetime.now(UTC)
        queued = self._scripts.get((order.symbol, order.side))
        steps: deque[FillStep] = deque(queued.popleft().steps) if queued else deque()
        self._orders[oid] = _Order(oid, order, submitted_at, steps=steps, full_close=full_close)
        return OrderAck(
            order_id=oid,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            status="new",
            submitted_at=submitted_at,
        )

    def script(self, symbol: str, side: Side, plan: FillPlan) -> None:
        """Queue `plan` for the next order submitted for (symbol, side), FIFO (spec section 5)."""
        self._scripts.setdefault((symbol, side), deque()).append(plan)

    async def cancel_order(self, order_id: str) -> None:
        o = self._get(order_id)
        if not o.is_open:
            raise OrderRejected(f"order {order_id} is not open ({o.status})")
        o.status = "canceled"  # final; filled_qty is kept, the remainder is released

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
        rules = self.asset_rules_by_symbol[symbol]
        if qty < rules.min_qty:  # unsellable remainder: stays as dust (HeldPosition.is_dust)
            raise OrderRejected(f"dust below minimum: {qty} < min_qty {rules.min_qty}")
        return await self._submit(
            BrokerOrder(symbol=symbol, side="sell", qty=qty, type="market"), full_close=True
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

    def redeliver(self, fill_id: str) -> None:
        """Put an already emitted fill on the stream again (same `fill_id`), for duplicate
        and out-of-order delivery tests. It never creates a new execution. Unknown ids
        raise KeyError."""
        self._fills.put_nowait(self._fill_log[fill_id])

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
        for o in [o for o in self._orders.values() if o.is_open]:
            if o.order.symbol == bar.symbol and bar.ts > o.submitted_at:
                self._on_bar(o, bar)
                if o.is_open and o.order.time_in_force == "ioc":
                    o.status = "canceled"  # IOC: only the first bar after submit counts
        self._last_price[bar.symbol] = _dec(bar.close)
        self._now = bar.ts if self._now is None else max(self._now, bar.ts)
        return True

    def _on_bar(self, o: _Order, bar: Bar) -> None:
        priced = self._price(o.order, bar)
        if priced is None:
            return  # limit not traded through: no step is consumed
        price, fee_bps = priced
        step: FillStep = o.steps.popleft() if o.steps else Decimal(1)
        if step == "cancel":
            o.status = "canceled"
        elif step == "reject":
            o.status, o.reason = "rejected", "scripted rejection"
        else:
            qty = self._slice(o, step)  # narrowed to Decimal by the branches above
            if qty > 0:
                self._fill(o, qty, price, fee_bps, bar.ts)

    def _price(self, order: BrokerOrder, bar: Bar) -> tuple[Decimal, Decimal] | None:
        """Fill price and fee rate in bps for `order` on `bar`, or None if it can't fill."""
        open_ = _dec(bar.open)
        if order.type == "market":
            slip = self.slippage_bps / _BPS
            price = open_ * (1 + slip) if order.side == "buy" else open_ * (1 - slip)
            return price, self.fees.taker_bps
        limit = cast(Decimal, order.limit_price)  # BrokerOrder guarantees it for limits
        if order.side == "buy" and _dec(bar.low) <= limit:
            price = min(open_, limit)
        elif order.side == "sell" and _dec(bar.high) >= limit:
            price = max(open_, limit)
        else:
            return None
        # An IOC limit crosses the book at submit (marketable limit): taker, not maker.
        ioc = order.time_in_force == "ioc"
        return price, self.fees.taker_bps if ioc else self.fees.maker_bps

    def _slice(self, o: _Order, fraction: Decimal) -> Decimal:
        """`fraction` of the original qty, rounded down and capped at the remainder. An
        off-increment full close rounds to `coin_precision` instead (spec section 5)."""
        increment = self.asset_rules_by_symbol[o.order.symbol].qty_increment
        step = increment if o.order.qty % increment == 0 else self.coin_precision
        return min(_floor(fraction * o.order.qty, step), o.remaining)

    def _fill(
        self, o: _Order, qty: Decimal, price: Decimal, fee_bps: Decimal, ts: datetime
    ) -> None:
        order = o.order
        notional = price * qty
        rate = fee_bps / _BPS
        if order.side == "buy" and self.fees.buy_fee_asset == "base":
            # Alpaca takes the buy fee from the coin at fill (G10, fee check step 1),
            # rounded up; cash pays exactly the notional.
            fee_asset: Literal["base", "quote"] = "base"
            fee = _ceil(qty * rate, self.coin_precision)
            cost, received = notional, qty - fee
            if received <= 0:
                o.status, o.reason = "rejected", "fill too small for coin fee"
                return
        else:
            fee_asset, fee = "quote", notional * rate
            cost, received = notional + fee, qty
        if order.side == "buy":
            if cost > self.cash:
                o.status, o.reason = "rejected", "insufficient cash at fill"
                return
            self.cash -= cost
            held = self._holdings.get(order.symbol)
            if held is None:
                self._holdings[order.symbol] = _Holding(received, price)
            else:
                # Weighted by coins held at fill prices: Alpaca keeps avg_entry_price = p.
                total = held.qty + received
                held.avg_entry_price = (held.qty * held.avg_entry_price + received * price) / total
                held.qty = total
        else:
            held = self._holdings[order.symbol]  # reserved at submit; cannot be missing
            self.cash += notional - fee
            held.qty -= qty
            if held.qty == 0:
                del self._holdings[order.symbol]
        o.filled_qty += qty
        o.filled_notional += notional
        o.status = "filled" if o.remaining == 0 else "partially_filled"
        o.fills += 1
        fill = Fill(
            fill_id=f"{o.id}:{o.fills}",
            order_id=o.id,
            symbol=order.symbol,
            side=order.side,
            qty=qty,
            price=price,
            fee=fee,
            fee_asset=fee_asset,
            ts=ts,
        )
        self._fill_log[fill.fill_id] = fill
        self._fills.put_nowait(fill)

    def _validate(self, order: BrokerOrder, *, full_close: bool = False) -> None:
        rules = self.asset_rules_by_symbol.get(order.symbol)
        if rules is None:
            raise OrderRejected(f"unknown symbol {order.symbol}")
        last = self._last_price.get(order.symbol)
        if last is None:
            raise OrderRejected(f"no price for {order.symbol} yet")
        if not full_close and order.qty % rules.qty_increment != 0:
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
            if self._estimated_cost(order, last, order.qty) > available:
                raise OrderRejected(f"insufficient cash: {available} available")
        else:
            held = self._holdings.get(order.symbol)
            free = (held.qty if held else Decimal(0)) - self._pending_qty(order.symbol, "sell")
            if order.qty > free:
                raise OrderRejected(f"sell qty {order.qty} exceeds position ({free} free)")

    def _estimated_cost(self, order: BrokerOrder, last: Decimal, qty: Decimal) -> Decimal:
        if order.limit_price is not None:
            price = order.limit_price
            fee_bps = self.fees.taker_bps if order.time_in_force == "ioc" else self.fees.maker_bps
        else:
            price = last * (1 + self.slippage_bps / _BPS)
            fee_bps = self.fees.taker_bps
        if self.fees.buy_fee_asset == "base":  # the fee comes out of the coin, not cash
            return price * qty
        return price * qty * (1 + fee_bps / _BPS)

    def _reserved_cash(self) -> Decimal:
        """Estimated cost of the unfilled remainder of every open buy."""
        return sum(
            (
                self._estimated_cost(o.order, self._last_price[o.order.symbol], o.remaining)
                for o in self._orders.values()
                if o.is_open and o.order.side == "buy"
            ),
            Decimal(0),
        )

    def _pending_qty(self, symbol: str, side: str) -> Decimal:
        """Unfilled remainder of open orders for (symbol, side)."""
        return sum(
            (
                o.remaining
                for o in self._orders.values()
                if o.is_open and o.order.symbol == symbol and o.order.side == side
            ),
            Decimal(0),
        )

    def _get(self, order_id: str) -> _Order:
        try:
            return self._orders[order_id]
        except KeyError:
            raise OrderNotFound(order_id) from None
