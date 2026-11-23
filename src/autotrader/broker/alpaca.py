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
- Responses: decimals must be plain decimal JSON strings (`-?digits(.digits)?`; a JSON number
  is malformed: `Decimal(0.5)` would be its binary expansion) and in range (Fable review 2
  M2): equity, a position's qty and entry price non-negative, `0 <= qty_available <= qty`,
  one row per symbol, an order's `qty` and `filled_avg_price` positive and `filled_qty <=
  qty` where the body carries `qty`; account cash may be negative (margin). Timestamps must
  be timezone-aware, symbols go through
  `_slash_symbol`; a position's symbol must then match `BASE/QUOTE` (it goes into the
  `/assets/{symbol}` path, which `asset_rules` checks too). The positions body must be a JSON
  array of objects that each carry a known `asset_class` (an empty array means flat, so a
  `{}` or an unclassed or unknown-class row fails the read; Codex P2-1 and round 2). A status
  outside the spec 3.4 table raises `UnknownOrderStatus`, checked before `filled_qty` is
  decoded (from the submit ack it first logs
  `alpaca.ack_status_unknown` with the broker order id); a mapped status contradicted by
  `filled_qty` (`filled` with none, any negative) raises `BrokerError` (Codex P2-2), so the
  read fails and the row stays unsettled. The submit ack must echo the
  order (client id, symbol, side, type, TIF, qty, limit price);
  a difference logs `alpaca.ack_mismatch` with the broker order id and raises `BrokerError`.
  Order ids (ack, `get_order_by_client_id`, and every id `get_order` / `cancel_order` put
  into a path) must be Alpaca's UUID form (`_ORDER_ID`, Codex P2-3).
  A looked-up order's client id and symbol are withheld when off-pattern (not raised, so a
  strange order under our client id stays a mismatch, not a failed lookup).
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
# would make `DELETE /orders` Alpaca's cancel-all (review M6). It also goes into log lines
# (`alpaca.http`, `alpaca.ack_*`, the engine's) and the journal unredacted, so it must be
# Alpaca's UUID form (Codex P2-3, PR #43, owner): a UUID has no dash-free run longer than 12
# characters, so it cannot contain a 20-character key id or a 40-character secret, and no log
# site needs to redact it.
_ORDER_ID = re.compile(
    r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}"
)
# A symbol goes into a URL path (`/assets/{symbol}`) and exception text (Fable review M1): the
# config's `Symbol` pattern. A client order id from a response reaches `repair link`'s output
# (Fable review M2); ours are `at-<jid>-...`, Alpaca allows up to 128 characters.
_SYMBOL = re.compile(r"[A-Z0-9]+/[A-Z0-9]+")
_CLIENT_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")

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


def _safe_symbol(raw: str) -> str:
    """`_slash_symbol`, and the result must match `_SYMBOL` (Fable review M1): a position's
    symbol goes into the `/assets/{symbol}` path and exception text."""
    mapped = _slash_symbol(raw)
    _check_symbol(mapped, shown=len(raw))
    return mapped


def _check_symbol(symbol: str, *, shown: int | None = None) -> None:
    """Refuse a symbol off `_SYMBOL` before it goes into a path; the message withholds it."""
    if _SYMBOL.fullmatch(symbol) is None:
        n = len(symbol) if shown is None else shown
        raise BrokerError(f"refusing an unsafe symbol: <withheld: {n} chars>") from None


# alpaca-py 0.44 `AssetClass`; only `crypto` rows are holdings here, the rest are skipped.
_ASSET_CLASSES = frozenset({"crypto", "us_equity", "us_option", "crypto_perp"})


def _known_class(raw: object) -> bool:
    return isinstance(raw, str) and raw in _ASSET_CLASSES


_MAPPED_STATUSES = frozenset(
    {
        "pending_new",
        "accepted",
        "new",
        "pending_cancel",
        "partially_filled",
        "filled",
        "canceled",
        "expired",
        "rejected",
    }
)


def order_state(raw: str, filled_qty: Decimal, request_id: str | None = None) -> OrderState:
    """Alpaca order status -> ours (spec 3.4). Anything unlisted raises `UnknownOrderStatus`:
    we never place orders that should reach it, so fail closed rather than guess.

    Pure: the exception carries `raw` as given. `AlpacaBroker._state` classifies the status
    itself before decoding `filled_qty` and raises with it filtered through `AlpacaHttp.diag`.

    For a mapped status, a negative `filled_qty`, or `filled` / `partially_filled` without a
    positive one, contradicts itself and raises a plain `BrokerError`: the read failed, the row
    stays unsettled (Codex P2-2, PR #43). The message names only a mapped status. An unlisted
    status is checked first, so it always takes the bounded `unknown_status` path whatever its
    `filled_qty`."""
    if raw not in _MAPPED_STATUSES:
        raise UnknownOrderStatus(raw, request_id)
    if filled_qty < 0:
        raise BrokerError(
            f"alpaca order reports a negative filled_qty (request {request_id})"
        ) from None
    if raw in ("filled", "partially_filled") and filled_qty == 0:
        raise BrokerError(
            f"alpaca order status {raw!r} with no filled_qty (request {request_id})"
        ) from None
    if raw in ("pending_new", "accepted", "new", "pending_cancel"):
        return "partially_filled" if filled_qty > 0 else "new"
    if raw == "partially_filled":
        return "partially_filled"
    if raw == "filled":
        return "filled"
    if raw in ("canceled", "expired"):
        return "canceled"
    return "rejected"


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
            # Cash is not range-checked: a margin account reports it below zero once buying
            # power beyond cash is spent. Equity below zero is not a state to trade on.
            return Account(equity=_non_negative(raw["equity"]), cash=_decimal(raw["cash"]))

    async def get_positions(self) -> list[Position]:
        result = await self._http.get("/positions", "/positions")
        raw = _body(result, "positions")
        # An empty list claims the account holds nothing, and the Position Manager's flatten
        # check reads it so: only a JSON array of rows that each name a known `asset_class`
        # is a holdings list (Codex P2-1, PR #43). Only the known non-spot classes are
        # skipped; a blank or unknown class fails the read rather than hide a holding (Codex
        # round 2). A class Alpaca adds later fails every positions read until it is listed.
        if not isinstance(raw, list) or not all(
            isinstance(p, dict) and _known_class(p.get("asset_class")) for p in raw
        ):
            raise BrokerError(
                "unexpected alpaca positions response: not a list of positions with a "
                f"known asset_class (request {result.request_id})"
            ) from None
        with _parsing("positions", result.request_id):
            positions = [_position(p) for p in raw if p.get("asset_class") == "crypto"]
            if len({p.symbol for p in positions}) != len(positions):
                raise ValueError("two positions for one symbol")
            return positions

    async def asset_rules(self, symbol: str) -> AssetRules:
        _check_symbol(symbol)
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
            state, filled = self._state(raw, rid)
            avg = raw.get("filled_avg_price")
            return OrderStatus(
                order_id=order_id,
                status=state,
                filled_qty=filled,
                avg_fill_price=None if avg is None else _positive(avg),
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
            order_id = _str(raw["id"])
            _check_order_id(order_id)  # it goes into a path (`get_order`) and log lines
            state, filled = self._state(raw, rid)
            limit = raw.get("limit_price")
            return BrokerOrderView(
                order_id=order_id,
                client_order_id=self._http.diag(_str(raw["client_order_id"]), _CLIENT_ID),
                symbol=_view_symbol(_str(raw["symbol"])),
                side=raw["side"],
                qty=_decimal(raw["qty"]),
                type=raw["type"],
                limit_price=None if limit is None else _decimal(limit),
                time_in_force=raw["time_in_force"],
                status=state,
                filled_qty=filled,
                created_at=_aware(raw["created_at"]),
            )

    async def order_fills(self, order_id: str) -> list[Fill]:
        raise OrderMethodsUnavailable("AlpacaBroker.order_fills is not implemented until WP 1.6b")

    def _writes_enabled(self) -> None:
        if not self._orders_enabled:
            raise OrderMethodsUnavailable("AlpacaBroker order writes are disabled until WP 1.6d")

    def _state(self, raw: Any, request_id: str | None) -> tuple[OrderState, Decimal]:
        """The order's state and `filled_qty` from a response body. The status is classified
        before the quantity is decoded, so an unmapped status raises `UnknownOrderStatus`
        (the bounded `unknown_status` path) even when `filled_qty` is missing or unparsable
        (Codex round 2, PR #43). Its status is filtered through `diag`, so the message and
        `.status` (which the engine logs) carry no raw response text (Codex r2)."""
        status = _str(raw["status"])
        if status not in _MAPPED_STATUSES:
            raise UnknownOrderStatus(self._http.diag(status, STATUS_RE), request_id) from None
        filled = _decimal(raw.get("filled_qty"))
        state = order_state(status, filled, request_id)
        qty = raw.get("qty")  # every order we place has one; checked where the body carries it
        if qty is not None and filled > _positive(qty):
            raise ValueError("filled_qty exceeds qty")
        return state, filled

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
                status, _ = self._state(raw, rid)
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


def _view_symbol(raw: str) -> str:
    """A looked-up order's symbol; one that does not map is withheld, not raised (Fable
    review M2): the order exists under our client id, so the caller's match check must see
    a difference (`client_id_mismatch`), not a failed lookup (`lookup_failed`)."""
    try:
        return _safe_symbol(raw)
    except BrokerError:
        return f"<withheld: {len(raw)} chars>"


def _check_order_id(order_id: str) -> None:
    # The id is withheld: in the ack check and the client-id lookup it is response data, and
    # an off-pattern one may embed a credential (Codex P2-3).
    if not _ORDER_ID.fullmatch(order_id):
        raise BrokerError(
            f"refusing an unsafe order id: <withheld: {len(order_id)} chars>"
        ) from None


def _str(raw: object) -> str:
    if not isinstance(raw, str):
        raise TypeError(f"expected a string, got {type(raw).__name__}")
    return raw


# A plain decimal string, the form every recorded paper response uses (Fable review 2 M2):
# `Decimal()` would also take whitespace, `_` separators, a sign, an exponent, non-ASCII
# digits, NaN and Infinity.
_PLAIN_DECIMAL = re.compile(r"-?[0-9]+(?:\.[0-9]+)?")


def _decimal(raw: object) -> Decimal:
    """Money/qty fields arrive as JSON strings; a JSON number has already lost precision.
    Anything but a plain decimal string is a `ValueError` (the value stays out of it)."""
    text = _str(raw)
    if _PLAIN_DECIMAL.fullmatch(text) is None:
        raise ValueError("not a plain decimal string")
    return Decimal(text)


def _non_negative(raw: object) -> Decimal:
    value = _decimal(raw)
    if value < 0:
        raise ValueError("negative where a non-negative value belongs")
    return value


def _positive(raw: object) -> Decimal:
    value = _decimal(raw)
    if value <= 0:
        raise ValueError("not positive where a positive value belongs")
    return value


def _position(raw: dict[str, Any]) -> Position:
    """One crypto row: `0 <= qty_available <= qty`, a non-negative entry price (Fable review 2
    M2: a negative qty would read as dust and let a flatten check pass)."""
    qty = _non_negative(raw["qty"])
    available = _non_negative(raw["qty_available"])
    if available > qty:
        raise ValueError("qty_available exceeds qty")
    return Position(
        symbol=_safe_symbol(_str(raw["symbol"])),
        qty=qty,
        qty_available=available,
        avg_entry_price=_non_negative(raw["avg_entry_price"]),
    )


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
    `order_state`'s fill contradiction, the ack mismatch) passes unchanged. Every raise is
    `from None`."""
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
