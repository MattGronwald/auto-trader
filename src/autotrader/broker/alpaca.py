"""Alpaca trading adapter (SPEC §4.6; WP 1.6a, spec 2026-11-12 alpaca-order-methods-design).

Facts verified 2026-10-11 and 2026-11-12 against docs.alpaca.markets and the paper API; see
docs/alpaca-notes.md for sources, raw values and open questions.

- Every trading-API call goes through our own async transport (`broker/alpaca_http.py`):
  fixed timeouts, no retries, no SDK, no worker threads. Cancelling the caller stops the
  request where it is; cancellation is not an `Exception` and passes through.
- One error contract, the spec 3.3 table: the transport maps exceptions (`RequestNotSent` =
  provably nothing written, any other failure `BrokerError`), this module maps HTTP status
  codes per method family. Every `BrokerError` raised from a response, `OrderNotFound`,
  `OrderRejected` and `UnknownOrderStatus` included, names Alpaca's request id and is
  raised `from None` (a rendered traceback has no cause chain). Alpaca's `message` passes
  through `sanitize_message`, every other response-derived value through `AlpacaHttp.diag`.
- Responses: decimals must be JSON strings (a JSON number is malformed: `Decimal(0.5)` would
  be its binary expansion), timestamps must be timezone-aware, symbols go through
  `_slash_symbol`. A status outside the spec 3.4 table raises `UnknownOrderStatus` (from the
  submit ack it first logs `alpaca.ack_status_unknown` with the broker order id). The
  submit ack must echo the order (client id, symbol, side, type, TIF, qty, limit price);
  a difference logs `alpaca.ack_mismatch` with the broker order id and raises `BrokerError`.
  Order ids from responses (ack, `get_order_by_client_id`) must match `[A-Za-z0-9-]+`.
- Order writes (`submit_order`, `cancel_order`) are refused before any I/O unless
  `orders_enabled`, which `connect()` never sets (enabling is WP 1.6d; paper only).
  `order_fills` lands in WP 1.6b. Reads of one order work from 1.6a on (`repair link`).
- `asset_rules` is cached per symbol for `ASSET_RULES_TTL_S` on an injected monotonic clock
  (request budget, spec 3.1); a failed fetch caches nothing.
- Fees are volume-tiered; `fee_schedule()` follows the account's `crypto_tier`.
- None is published, but paper refuses buys worth under $10 at their limit price (probed
  2026-11-12, docs/alpaca-notes.md); `min_notional` stays 0 until WP 1.6c. Max $200k/order.
"""

import re
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx
import structlog

from autotrader.broker.alpaca_http import (
    FIELD_RE,
    LIVE_URL,
    PAPER_URL,
    STATUS_RE,
    AlpacaHttp,
    HttpResult,
    wire_decimal,
)
from autotrader.broker.base import (
    Account,
    AssetRules,
    BrokerError,
    BrokerOrder,
    BrokerOrderView,
    FeeSchedule,
    Fill,
    Mode,
    OrderAck,
    OrderMethodsUnavailable,
    OrderNotFound,
    OrderRejected,
    OrderState,
    OrderStatus,
    Position,
    UnknownOrderStatus,
)
from autotrader.core.config import Settings

# 30-day-volume fee tiers (maker, taker in bps), docs verified 2026-10-11.
FEE_TIERS: dict[int, FeeSchedule] = {
    tier: FeeSchedule(maker_bps=Decimal(maker), taker_bps=Decimal(taker), buy_fee_asset="base")
    for tier, (maker, taker) in {
        1: (15, 25),
        2: (12, 22),
        3: (10, 20),
        4: (8, 18),
        5: (5, 15),
        6: (2, 13),
        7: (2, 12),
        8: (0, 10),
    }.items()
}
ASSET_RULES_TTL_S = 300.0
# An order id goes into a URL path: httpx normalises `/orders/../account`, and an empty id
# would make `DELETE /orders` Alpaca's cancel-all (review M6).
_ORDER_ID = re.compile(r"[A-Za-z0-9-]+")

log = structlog.get_logger(__name__)


def _slash_symbol(raw: str) -> str:
    """Positions may report crypto as `BTCUSD`; the rest of the system uses `BTC/USD`.

    The raw symbol stays out of the error message (Codex r3): it is response data."""
    if "/" in raw:
        return raw
    for quote in ("USD", "USDT", "USDC", "BTC"):
        if raw.endswith(quote) and len(raw) > len(quote):
            return f"{raw[: -len(quote)]}/{quote}"
    raise BrokerError(f"cannot map position symbol to BASE/QUOTE: <withheld: {len(raw)} chars>")


def order_state(raw: str, filled_qty: Decimal, request_id: str | None = None) -> OrderState:
    """Alpaca order status -> ours (spec 3.4). Anything unlisted raises `UnknownOrderStatus`:
    we never place orders that should reach it, so fail closed rather than guess.

    Pure: the exception carries `raw` as given. Callers in this module re-raise it with the
    status filtered through `AlpacaHttp.diag` (`AlpacaBroker._state`)."""
    if raw in ("pending_new", "accepted", "new", "pending_cancel"):
        return "partially_filled" if filled_qty > 0 else "new"
    if raw == "partially_filled":
        return "partially_filled"
    if raw == "filled":
        return "filled"
    if raw in ("canceled", "expired"):
        return "canceled"
    if raw == "rejected":
        return "rejected"
    raise UnknownOrderStatus(raw, request_id)


class AlpacaBroker:
    def __init__(
        self,
        http: AlpacaHttp,
        *,
        paper: bool,
        fees: FeeSchedule,
        orders_enabled: bool = False,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if orders_enabled and not paper:
            raise BrokerError("order writes are enabled for paper accounts only")
        self._http = http
        self._paper = paper
        self._fees = fees
        self._orders_enabled = orders_enabled
        self._monotonic = monotonic
        self._rules: dict[str, tuple[float, AssetRules]] = {}

    @classmethod
    async def connect(
        cls, settings: Settings, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> "AlpacaBroker":
        """Build a broker from settings and check the account can trade crypto.

        `transport` is for tests. Live mode additionally requires ALLOW_LIVE=1 (SPEC §12), on
        top of the same check at config load. Both checks run before any client exists; if
        the account check fails or is cancelled, both clients are closed before re-raising.
        Order writes stay off: nothing here can enable them (spec 3.8).
        """
        paper = settings.alpaca_paper
        if not paper and not settings.allow_live:
            raise BrokerError("live Alpaca account requested but ALLOW_LIVE=1 is not set")
        if settings.alpaca_api_key_id is None or settings.alpaca_api_secret_key is None:
            raise BrokerError("ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY are not set")
        http = AlpacaHttp(
            base_url=PAPER_URL if paper else LIVE_URL,
            key_id=settings.alpaca_api_key_id.get_secret_value(),
            secret_key=settings.alpaca_api_secret_key.get_secret_value(),
            transport=transport,
        )
        try:
            result = await http.get("/account", "/account")
            account = _body(result, "account")
            rid = result.request_id
            with _parsing("account", rid):
                status = account.get("crypto_status")
                tier = account.get("crypto_tier")
            if str(status).upper() != "ACTIVE":
                raise BrokerError(
                    "crypto trading not active on this account: "
                    f"{http.diag(status, FIELD_RE)} (request {rid})"
                )
            if tier not in FEE_TIERS:
                raise BrokerError(
                    f"unknown crypto_tier {http.diag(tier, FIELD_RE)} (request {rid}); "
                    "update FEE_TIERS from the docs"
                )
        except BaseException:
            await http.aclose()
            raise
        return cls(http, paper=paper, fees=FEE_TIERS[tier])

    async def aclose(self) -> None:
        """Close both HTTP clients (the runtime's teardown and `cli_repair` call it)."""
        await self._http.aclose()

    # --- read side -------------------------------------------------------------------

    async def get_account(self) -> Account:
        result = await self._http.get("/account", "/account")
        raw = _body(result, "account")
        with _parsing("account", result.request_id):
            return Account(equity=_decimal(raw["equity"]), cash=_decimal(raw["cash"]))

    async def get_positions(self) -> list[Position]:
        result = await self._http.get("/positions", "/positions")
        raw = _body(result, "positions")
        with _parsing("positions", result.request_id):
            return [
                Position(
                    symbol=_slash_symbol(_str(p["symbol"])),
                    qty=_decimal(p["qty"]),
                    qty_available=_decimal(p["qty_available"]),
                    avg_entry_price=_decimal(p["avg_entry_price"]),
                )
                for p in raw
                if p.get("asset_class") == "crypto"
            ]

    async def asset_rules(self, symbol: str) -> AssetRules:
        now = self._monotonic()
        cached = self._rules.get(symbol)
        if cached is not None and now < cached[0]:
            return cached[1]
        result = await self._http.get("/assets/{symbol}", f"/assets/{symbol}")
        raw = _body(result, f"asset {symbol}")
        with _parsing(f"asset {symbol}", result.request_id):
            rules = AssetRules(
                min_qty=_decimal(raw["min_order_size"]),
                qty_increment=_decimal(raw["min_trade_increment"]),
                min_notional=Decimal(0),
                price_increment=_decimal(raw["price_increment"]),
            )
        self._rules[symbol] = (now + ASSET_RULES_TTL_S, rules)
        return rules

    def fee_schedule(self) -> FeeSchedule:
        return self._fees

    def can_place_orders(self) -> bool:
        return self._orders_enabled

    def mode(self) -> Mode:
        return "paper" if self._paper else "live"

    # --- order side ------------------------------------------------------------------

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        self._writes_enabled()
        payload = {
            "symbol": order.symbol,
            "side": order.side,
            "type": order.type,
            "qty": wire_decimal(order.qty),
            "time_in_force": order.time_in_force,
        }
        if order.client_order_id is not None:
            payload["client_order_id"] = order.client_order_id
        if order.limit_price is not None:
            payload["limit_price"] = wire_decimal(order.limit_price)
        result = await self._http.post("/orders", "/orders", payload)
        rid = result.request_id
        if result.status == 200 and result.parsed:
            return self._ack(order, result)
        if result.status == 403 and result.parsed:
            raw_message = result.body.get("message") if isinstance(result.body, dict) else None
            message = (
                "<no message>" if raw_message is None else self._http.sanitize_message(raw_message)
            )
            raise OrderRejected(f"alpaca rejected the order: {message} (request {rid})")
        raise BrokerError(f"alpaca submit_order returned {result.status} (request {rid})")

    async def cancel_order(self, order_id: str) -> None:
        self._writes_enabled()
        _check_order_id(order_id)
        result = await self._http.delete("/orders/{id}", f"/orders/{order_id}", order_id=order_id)
        rid = result.request_id
        if 200 <= result.status < 300:
            return
        if result.status == 404:
            raise OrderNotFound(f"{order_id} (request {rid})")
        raise BrokerError(f"alpaca cancel_order returned {result.status} (request {rid})")

    async def get_order(self, order_id: str) -> OrderStatus:
        _check_order_id(order_id)
        result = await self._http.get("/orders/{id}", f"/orders/{order_id}", order_id=order_id)
        rid = result.request_id
        if result.status == 404:
            raise OrderNotFound(f"{order_id} (request {rid})")
        raw = _body(result, "get_order")
        with _parsing("get_order", rid):
            if raw["id"] != order_id:
                raise ValueError("the response is another order")
            filled = _decimal(raw["filled_qty"])
            avg = raw.get("filled_avg_price")
            return OrderStatus(
                order_id=order_id,
                status=self._state(raw["status"], filled, rid),
                filled_qty=filled,
                avg_fill_price=None if avg is None else _decimal(avg),
                reason=None,
            )

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None:
        result = await self._http.get(
            "/orders:by_client_order_id",
            "/orders:by_client_order_id",
            params={"client_order_id": client_order_id},
        )
        if result.status == 404:
            return None
        raw = _body(result, "get_order_by_client_id")
        rid = result.request_id
        with _parsing("get_order_by_client_id", rid):
            filled = _decimal(raw["filled_qty"])
            limit = raw.get("limit_price")
            order_id = _str(raw["id"])
            _check_order_id(order_id)  # it goes into a path (`get_order`) and log lines
            return BrokerOrderView(
                order_id=order_id,
                client_order_id=_str(raw["client_order_id"]),
                symbol=_slash_symbol(_str(raw["symbol"])),
                side=raw["side"],
                qty=_decimal(raw["qty"]),
                type=raw["type"],
                limit_price=None if limit is None else _decimal(limit),
                time_in_force=raw["time_in_force"],
                status=self._state(raw["status"], filled, rid),
                filled_qty=filled,
                created_at=_aware(raw["created_at"]),
            )

    async def order_fills(self, order_id: str) -> list[Fill]:
        raise OrderMethodsUnavailable("AlpacaBroker.order_fills is not implemented until WP 1.6b")

    def _writes_enabled(self) -> None:
        if not self._orders_enabled:
            raise OrderMethodsUnavailable("AlpacaBroker order writes are disabled until WP 1.6d")

    def _state(self, raw: object, filled_qty: Decimal, request_id: str | None) -> OrderState:
        """`order_state` on the raw status; an unmapped one is re-raised with the status
        filtered through `diag`, so the message and `.status` (which the engine logs) carry
        no raw response text (Codex r2)."""
        try:
            return order_state(_str(raw), filled_qty, request_id)
        except UnknownOrderStatus as e:
            raise UnknownOrderStatus(self._http.diag(e.status, STATUS_RE), request_id) from None

    def _ack(self, order: BrokerOrder, result: HttpResult) -> OrderAck:
        """Validate a 200 submit response against the order we sent (spec 3.4)."""
        raw, rid = result.body, result.request_id
        with _parsing("submit_order", rid):
            order_id = raw["id"]
            if not isinstance(order_id, str) or not order_id:
                raise ValueError("the ack has no order id")
            _check_order_id(order_id)
            limit = raw["limit_price"]
            got: dict[str, object] = {
                "client_order_id": raw.get("client_order_id"),
                "symbol": _slash_symbol(_str(raw["symbol"])),
                "side": raw["side"],
                "type": raw["type"],
                "time_in_force": raw["time_in_force"],
                "qty": _decimal(raw["qty"]),
                "limit_price": None if limit is None else _decimal(limit),
            }
            sent: dict[str, object] = {
                "client_order_id": order.client_order_id,
                "symbol": order.symbol,
                "side": order.side,
                "type": order.type,
                "time_in_force": order.time_in_force,
                "qty": order.qty,
                "limit_price": order.limit_price,
            }
            if order.client_order_id is None:  # Alpaca assigns its own id; nothing to compare
                del got["client_order_id"], sent["client_order_id"]
            diag = self._http.diag
            fields = [
                f"{name}: {sent[name]} != {diag(got[name], FIELD_RE)}"
                for name in sent
                if sent[name] != got[name]
            ]
            if fields:
                log.error(
                    "alpaca.ack_mismatch",
                    order_id=diag(order_id, FIELD_RE),
                    fields=fields,
                    request_id=rid,
                )
                raise BrokerError(f"alpaca ack does not match the order (request {rid})")
            try:
                status = self._state(raw["status"], _decimal(raw["filled_qty"]), rid)
            except UnknownOrderStatus as e:  # the order is live; the operator needs its id
                log.error(
                    "alpaca.ack_status_unknown",
                    order_id=diag(order_id, FIELD_RE),
                    status=e.status,  # already filtered by `_state`
                    request_id=rid,
                )
                raise
            submitted = raw.get("submitted_at") or raw["created_at"]
            cid = raw.get("client_order_id")
            return OrderAck(
                order_id=order_id,
                client_order_id=cid if isinstance(cid, str) else None,
                symbol=order.symbol,
                status=status,
                submitted_at=_aware(submitted),
            )


def _body(result: HttpResult, what: str) -> Any:
    """The parsed body of a 2xx; anything else is a `BrokerError` with the request id."""
    if 200 <= result.status < 300 and result.parsed:
        return result.body
    raise BrokerError(f"alpaca {what} returned {result.status} (request {result.request_id})")


def _check_order_id(order_id: str) -> None:
    # The id is withheld: in the ack check and the client-id lookup it is response data.
    if not _ORDER_ID.fullmatch(order_id):
        raise BrokerError(f"refusing an unsafe order id: <withheld: {len(order_id)} chars>")


def _str(raw: object) -> str:
    if not isinstance(raw, str):
        raise TypeError(f"expected a string, got {type(raw).__name__}")
    return raw


def _decimal(raw: object) -> Decimal:
    """Money/qty fields arrive as JSON strings; a JSON number has already lost precision."""
    return Decimal(_str(raw))


def _aware(raw: object) -> datetime:
    """An ISO timestamp with a zone (`Z` and nanoseconds accepted, truncated to microseconds)."""
    value = datetime.fromisoformat(_str(raw))
    if value.tzinfo is None:
        raise ValueError("naive timestamp")
    return value


@contextmanager
def _parsing(what: str, request_id: str | None) -> Iterator[None]:
    """A response with missing, mistyped or non-numeric fields is a broker failure, not a
    crash. The message carries the cause's type name and the request id, never the cause's
    text (it can quote response data). A nested `BrokerError` (`_slash_symbol`, an unsafe
    ack id) gets the request id appended; one that already names it (`UnknownOrderStatus`,
    the ack mismatch) passes unchanged. Every raise is `from None`."""
    try:
        yield
    except BrokerError as e:
        if "(request " in str(e):
            raise
        raise BrokerError(f"{e} (request {request_id})") from None
    except (KeyError, TypeError, ArithmeticError, ValueError, AttributeError) as e:
        raise BrokerError(
            f"unexpected alpaca {what} response: {type(e).__name__} (request {request_id})"
        ) from None
