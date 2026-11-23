"""AlpacaBroker over `httpx.MockTransport` with recorded paper responses (no network).

Acceptance items of the WP 1.6a spec (3.10) are named in the tests; `Router`
(`alpaca_mock.py`) answers queued responses and records every request.
"""

import asyncio
import io
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from autotrader.broker import alpaca
from autotrader.broker.alpaca import (
    ASSET_RULES_TTL_S,
    FEE_TIERS,
    AlpacaBroker,
    order_state,
)
from autotrader.broker.alpaca_http import LIVE_URL, PAPER_URL, AlpacaHttp
from autotrader.broker.base import (
    AssetRules,
    Broker,
    BrokerError,
    BrokerOrder,
    FeeSchedule,
    OrderMethodsUnavailable,
    OrderNotFound,
    OrderRejected,
    RequestNotSent,
    UnknownOrderStatus,
)
from autotrader.core.config import Settings
from autotrader.core.logging import configure_logging
from tests.unit.broker.alpaca_mock import ACCOUNT, KEY, SECRET, Router, order_from, probe

ENTRY = probe("E-entry")
CHANGED: dict[str, str] = {  # one wrong value per ack field (the E-entry probe's order)
    "client_order_id": "probe16a-someone-else",
    "symbol": "ETH/USD",
    "side": "sell",
    "type": "market",
    "time_in_force": "gtc",
    "qty": "0.000204027",  # plus one increment (1e-9)
    "limit_price": "73667.31052",  # plus one cent
}


async def broker(router: Router, *, orders_enabled: bool = True, **kw: Any) -> AlpacaBroker:
    http = AlpacaHttp(base_url=PAPER_URL, key_id=KEY, secret_key=SECRET, transport=router.transport)
    return AlpacaBroker(http, paper=True, fees=FEE_TIERS[1], orders_enabled=orders_enabled, **kw)


def settings(**kw: Any) -> Settings:
    return Settings(_env_file=None, alpaca_api_key_id=KEY, alpaca_api_secret_key=SECRET, **kw)


async def connect(router: Router, **kw: Any) -> AlpacaBroker:
    return await AlpacaBroker.connect(settings(**kw), transport=router.transport)


def account_router(account: dict[str, Any] | None = None) -> Router:
    r = Router()
    r.add("GET", "/v2/account", 200, account or ACCOUNT)
    return r


class Recorded(AlpacaHttp):
    """Records every instance `connect()` builds, to check that each one ends closed."""

    instances: list[AlpacaHttp]

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        Recorded.instances.append(self)


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> list[AlpacaHttp]:
    Recorded.instances = []
    monkeypatch.setattr(alpaca, "AlpacaHttp", Recorded)
    return Recorded.instances


# --- submit: acks, mismatches (acceptance 4) -------------------------------------------


async def test_recorded_entry_and_exit_acks_pass_unchanged() -> None:
    for run, step in (
        ("round1", "E-entry"),
        ("round1", "H-exit"),
        ("round2", "I-sell-half"),
        ("round2", "C2-ioc-nofill"),
        ("round2", "D2-gtc-rest"),
    ):
        entry = probe(step, run=run)
        r = Router()
        r.add("POST", "/v2/orders", 200, entry["json"])
        ack = await (await broker(r)).submit_order(order_from(entry["body"]))
        assert (ack.order_id, ack.status) == (entry["json"]["id"], "new")
        assert json.loads(r.requests[0].content) == entry["body"]  # exact wire shape
        assert r.unrouted == []


@pytest.mark.parametrize(
    "field", ["client_order_id", "symbol", "side", "type", "time_in_force", "qty", "limit_price"]
)
async def test_each_ack_field_mismatch_raises_and_logs_the_broker_id(field: str) -> None:
    entry = probe("E-entry")
    r = Router()
    r.add("POST", "/v2/orders", 200, {**entry["json"], field: CHANGED[field]})
    with capture_logs() as logs, pytest.raises(BrokerError) as e:
        await (await broker(r)).submit_order(order_from(entry["body"]))
    assert type(e.value) is BrokerError
    (line,) = [x for x in logs if x["event"] == "alpaca.ack_mismatch"]
    assert line["order_id"] == entry["json"]["id"] and line["fields"][0].startswith(field)


async def test_an_order_without_client_id_is_sent_without_one() -> None:
    body = {k: v for k, v in probe("H-exit")["body"].items() if k != "client_order_id"}
    ack_json = {**probe("H-exit")["json"], "client_order_id": "alpaca-assigned-1"}
    r = Router()
    r.add("POST", "/v2/orders", 200, ack_json)
    ack = await (await broker(r)).submit_order(order_from(body))
    assert "client_order_id" not in json.loads(r.requests[0].content)
    assert (ack.order_id, ack.client_order_id) == (ack_json["id"], "alpaca-assigned-1")
    assert r.unrouted == []


# Off Alpaca's UUID form (Codex P2-3, PR #43): ids that the old `[A-Za-z0-9-]+` check let
# through, KEY itself among them, plus a UUID with a byte too many or too few.
NON_UUID_IDS = [
    KEY,
    f"{KEY}-0000",
    "order-1",
    "00000000000040008000000000016a009",
    "00000000-0000-4000-8000-00000016a0090",
    "00000000-0000-4000-8000-00000016a00",
    "0000000g-0000-4000-8000-00000016a009",
]


@pytest.mark.parametrize("bad_id", ["", 7, None, "../account", *NON_UUID_IDS])
async def test_an_ack_without_a_usable_order_id_is_a_broker_error(bad_id: object) -> None:
    r = Router()
    r.add("POST", "/v2/orders", 200, {**ENTRY["json"], "id": bad_id})
    with pytest.raises(BrokerError, match="request rid-1") as e:
        await (await broker(r)).submit_order(order_from(ENTRY["body"]))
    assert type(e.value) is BrokerError
    assert r.unrouted == []


# --- reads of one order ----------------------------------------------------------------


async def test_get_order_with_another_id_is_a_broker_error() -> None:  # builder call, review I4
    resting = probe("D2-resting", run="round2")["json"]
    other = "00000000-0000-4000-8000-0000000000ff"
    r = Router()
    r.add("GET", f"/v2/orders/{other}", 200, resting)
    with pytest.raises(BrokerError, match="request rid-1"):
        await (await broker(r)).get_order(other)


async def test_get_order_and_the_client_id_lookup_map_the_recorded_orders() -> None:
    final = probe("E-final")["json"]
    visible = probe("E-visible")["json"]
    r = Router()
    r.add("GET", f"/v2/orders/{final['id']}", 200, final)
    r.add("GET", "/v2/orders:by_client_order_id", 200, visible)
    b = await broker(r)
    status = await b.get_order(final["id"])
    assert (status.status, status.filled_qty) == ("filled", Decimal(final["filled_qty"]))
    assert status.avg_fill_price == Decimal(final["filled_avg_price"])
    assert status.reason is None
    view = await b.get_order_by_client_id(visible["client_order_id"])
    assert view is not None
    assert (view.order_id, view.symbol, view.type, view.time_in_force, view.status) == (
        visible["id"],
        "BTC/USD",
        "limit",
        "ioc",
        "filled",
    )
    assert view.limit_price == Decimal(visible["limit_price"])
    assert view.created_at == datetime(2026, 11, 12, 10, 26, 3, 148318, tzinfo=UTC)
    assert r.requests[1].url.params["client_order_id"] == visible["client_order_id"]
    assert r.unrouted == []


async def test_a_resting_order_reads_without_a_fill_price() -> None:
    resting = probe("D2-resting", run="round2")["json"]
    r = Router()
    r.add("GET", f"/v2/orders/{resting['id']}", 200, resting)
    status = await (await broker(r)).get_order(resting["id"])
    assert (status.status, status.filled_qty, status.avg_fill_price) == ("new", Decimal(0), None)
    assert r.unrouted == []


@pytest.mark.parametrize("bad_id", ["../account", "a/b", f"id {KEY}", "", *NON_UUID_IDS])
async def test_the_client_id_lookup_refuses_an_unsafe_order_id(bad_id: str) -> None:
    # Final review M1: the returned id is checked like the ack's and get_order's; it later
    # goes into a path (`get_order`) and a log line (`risk.order_lookup_mismatch`).
    r = Router()
    r.add("GET", "/v2/orders:by_client_order_id", 200, {**probe("E-visible")["json"], "id": bad_id})
    with pytest.raises(BrokerError) as e:
        await (await broker(r)).get_order_by_client_id("at-0123abcd-c1-e")
    assert type(e.value) is BrokerError and "(request rid-1)" in str(e.value)
    assert f"<withheld: {len(bad_id)} chars>" in str(e.value)
    assert "account" not in str(e.value) and KEY not in str(e.value)
    assert e.value.__cause__ is None and e.value.__suppress_context__
    assert r.unrouted == []


@pytest.mark.parametrize("bad", ["../account", "", "a/b", "id?x=1", "id with space", *NON_UUID_IDS])
async def test_unsafe_order_ids_are_refused_before_io(bad: str) -> None:  # review M6, Codex P2-3
    r = Router()
    b = await broker(r)
    for call in (b.get_order(bad), b.cancel_order(bad)):
        with pytest.raises(BrokerError):
            await call
    assert r.requests == []


def test_order_ids_must_be_alpacas_uuid_form() -> None:
    # Codex P2-3: a UUID has no dash-free run longer than 12 characters, so it cannot contain
    # a 20-character key id or a 40-character secret; recorded ids pass, either case.
    assert alpaca._ORDER_ID.fullmatch(OID)
    assert alpaca._ORDER_ID.fullmatch(OID.upper())
    assert alpaca._ORDER_ID.fullmatch(ENTRY["json"]["id"])
    assert not any(alpaca._ORDER_ID.fullmatch(bad) for bad in NON_UUID_IDS)


@pytest.mark.parametrize("call", ["by_client_id", "submit"])
async def test_a_key_bearing_order_id_is_refused_and_never_rendered(call: str) -> None:
    # Codex P2-3 sentinel: a response whose order id embeds the key id is refused, nothing is
    # returned, and the production log chain renders neither credential.
    bad_id = f"{KEY}-0000"
    r = Router()
    if call == "submit":
        r.add("POST", "/v2/orders", 200, {**ENTRY["json"], "id": bad_id})
    else:
        r.add("GET", "/v2/orders:by_client_order_id", 200, {**ENTRY["json"], "id": bad_id})
    t = structlog.get_logger("t")
    buf = io.StringIO()
    configure_logging(stream=buf)  # production chain; tests/conftest.py restores it
    returned: object = None
    with pytest.raises(BrokerError) as e:
        try:
            returned = await _invoke(await broker(r), call)
        except BrokerError:
            t.exception("risk.order_lookup_failed")  # renders the traceback, as the engine does
            raise
    assert returned is None and type(e.value) is BrokerError
    rendered, text = buf.getvalue(), str(e.value)
    assert "alpaca.http" in rendered and "risk.order_lookup_failed" in rendered
    assert KEY not in rendered and SECRET not in rendered
    assert KEY not in text and f"<withheld: {len(bad_id)} chars>" in text
    assert r.unrouted == []


# --- writes switched off (acceptance 10) -----------------------------------------------


async def test_writes_are_refused_before_io_while_disabled() -> None:
    resting = probe("D2-resting", run="round2")["json"]
    oid = resting["id"]
    r = Router()
    r.add("GET", f"/v2/orders/{oid}", 200, resting)
    b = await broker(r, orders_enabled=False)
    with pytest.raises(OrderMethodsUnavailable):
        await b.submit_order(order_from(probe("E-entry")["body"]))
    with pytest.raises(OrderMethodsUnavailable):
        await b.cancel_order(oid)
    assert b.can_place_orders() is False and r.requests == []
    assert (await b.get_order(oid)).status == "new"  # reads still work


async def test_the_client_id_lookup_works_while_writes_are_disabled() -> None:
    r = Router()
    r.add("GET", "/v2/orders:by_client_order_id", 404, probe("A-get-unknown-cid")["json"])
    b = await broker(r, orders_enabled=False)
    assert await b.get_order_by_client_id("probe16a-20261112T102600-nonexistent") is None
    assert r.unrouted == []


def test_orders_enabled_with_a_live_account_is_refused() -> None:
    http = AlpacaHttp(base_url=LIVE_URL, key_id=KEY, secret_key=SECRET)
    with pytest.raises(BrokerError, match="paper accounts only"):
        AlpacaBroker(http, paper=False, fees=FEE_TIERS[1], orders_enabled=True)


async def test_connect_never_enables_writes() -> None:
    b = await connect(account_router())
    assert b.can_place_orders() is False
    await b.aclose()


# --- asset rules cache (acceptance 12) and lifecycle ------------------------------------


async def test_asset_rules_are_cached_for_five_minutes() -> None:
    now = [0.0]
    r = Router()
    r.add("GET", "/v2/assets/BTC/USD", 200, probe("0-asset")["json"], repeat=True)
    b = await broker(r, monotonic=lambda: now[0])
    await b.asset_rules("BTC/USD")
    now[0] = 299.0
    await b.asset_rules("BTC/USD")
    assert len(r.requests) == 1
    now[0] = 300.0
    await b.asset_rules("BTC/USD")
    assert len(r.requests) == 2


async def test_a_failed_asset_fetch_caches_nothing() -> None:
    r = Router()
    r.add("GET", "/v2/assets/BTC/USD", 500, {"message": "x"})
    r.add("GET", "/v2/assets/BTC/USD", 200, {**probe("0-asset")["json"], "min_order_size": 1})
    r.add("GET", "/v2/assets/BTC/USD", 200, probe("0-asset")["json"])
    b = await broker(r, monotonic=lambda: 0.0)
    for _ in range(2):  # a status error, then a malformed body
        with pytest.raises(BrokerError):
            await b.asset_rules("BTC/USD")
    assert (await b.asset_rules("BTC/USD")).min_qty == Decimal("0.00001357")
    assert ASSET_RULES_TTL_S == 300.0
    assert len(r.requests) == 3 and r.unrouted == []


async def test_aclose_closes_both_clients() -> None:  # review M11
    b = await broker(Router())
    await b.aclose()
    assert b._http.closed


# --- status contract (spec 3.3, acceptance 1) and status table (3.4, acceptance 5) ------

OID = "00000000-0000-4000-8000-00000016a009"
HTML = b"<html><body>maintenance</body></html>"

# (call, status, body, raised); body: dict = JSON, bytes = raw content, None = empty.
CONTRACT: list[tuple[str, int, Any, type[BrokerError] | None]] = [
    ("submit", 403, probe("B-below-min")["json"], OrderRejected),
    ("submit", 403, HTML, BrokerError),
    ("submit", 400, {"message": "bad"}, BrokerError),
    ("submit", 401, {"message": "unauthorized"}, BrokerError),
    ("submit", 404, {"message": "not found"}, BrokerError),
    ("submit", 422, probe("F-duplicate-cid")["json"], BrokerError),
    ("submit", 429, {"message": "rate limit"}, BrokerError),
    ("submit", 500, {"message": "x"}, BrokerError),
    ("submit", 503, None, BrokerError),
    ("submit", 201, ENTRY["json"], BrokerError),  # only a 200 is an ack
    ("get_order", 404, probe("A-get-unknown-id")["json"], OrderNotFound),
    ("get_order", 500, {"message": "x"}, BrokerError),
    ("get_order", 401, {"message": "unauthorized"}, BrokerError),
    ("by_client_id", 404, probe("A-get-unknown-cid")["json"], None),
    ("by_client_id", 500, {"message": "x"}, BrokerError),
    ("cancel", 204, None, None),
    ("cancel", 200, None, None),  # any 2xx returns
    ("cancel", 422, probe("G-cancel-final")["json"], BrokerError),
    ("cancel", 404, probe("A-cancel-unknown-id")["json"], OrderNotFound),
    ("cancel", 500, {"message": "x"}, BrokerError),
    ("account", 500, {"message": "x"}, BrokerError),
    ("account", 200, HTML, BrokerError),
    ("positions", 500, {"message": "x"}, BrokerError),
    ("asset", 404, {"message": "asset not found"}, BrokerError),
]
_ROUTES = {
    "submit": ("POST", "/v2/orders"),
    "get_order": ("GET", f"/v2/orders/{OID}"),
    "by_client_id": ("GET", "/v2/orders:by_client_order_id"),
    "cancel": ("DELETE", f"/v2/orders/{OID}"),
    "account": ("GET", "/v2/account"),
    "positions": ("GET", "/v2/positions"),
    "asset": ("GET", "/v2/assets/BTC/USD"),
}


async def _invoke(b: AlpacaBroker, call: str) -> object:
    if call == "submit":
        return await b.submit_order(order_from(ENTRY["body"]))
    if call == "get_order":
        return await b.get_order(OID)
    if call == "by_client_id":
        return await b.get_order_by_client_id("at-0123abcd-c1-e")
    if call == "cancel":
        await b.cancel_order(OID)
        return None
    if call == "positions":
        return await b.get_positions()
    if call == "asset":
        return await b.asset_rules("BTC/USD")
    return await b.get_account()


@pytest.mark.parametrize(("call", "status", "body", "raised"), CONTRACT)
async def test_status_contract(
    call: str, status: int, body: Any, raised: type[BrokerError] | None
) -> None:
    r = Router()
    method, path = _ROUTES[call]
    if isinstance(body, bytes):
        r.add(method, path, status, content=body)
    else:
        r.add(method, path, status, body)
    b = await broker(r)
    if raised is None:
        assert await _invoke(b, call) is None
    else:
        with pytest.raises(raised) as e:
            await _invoke(b, call)
        assert type(e.value) is raised  # exact class, not a subclass
        assert "request rid-1" in str(e.value)  # raised from a response: carries the id
        assert e.value.__cause__ is None
    assert r.unrouted == []


async def test_a_rejection_carries_alpacas_message_sanitised() -> None:  # Codex P2-1
    r = Router()
    r.add("POST", "/v2/orders", 403, probe("B-below-min")["json"])
    r.add("POST", "/v2/orders", 403, {"message": f"bad key {KEY} / {SECRET}\n" + "x" * 500})
    r.add("POST", "/v2/orders", 403, {"code": 40310000})
    b = await broker(r)
    with pytest.raises(OrderRejected, match="cost basis must be >= minimal amount of order 10"):
        await b.submit_order(order_from(ENTRY["body"]))
    with pytest.raises(OrderRejected) as e:
        await b.submit_order(order_from(ENTRY["body"]))
    text = str(e.value)
    assert KEY not in text and SECRET not in text and "\n" not in text
    assert "[redacted]" in text and "(request rid-2)" in text
    with pytest.raises(OrderRejected, match="<no message>"):
        await b.submit_order(order_from(ENTRY["body"]))


STATUS_TABLE: list[tuple[str, str, str]] = [
    ("pending_new", "0", "new"),
    ("pending_new", "0.5", "partially_filled"),
    ("accepted", "0", "new"),
    ("accepted", "0.5", "partially_filled"),
    ("new", "0", "new"),
    ("new", "0.5", "partially_filled"),
    ("partially_filled", "0.5", "partially_filled"),
    ("pending_cancel", "0", "new"),
    ("pending_cancel", "0.5", "partially_filled"),
    ("filled", "1", "filled"),
    ("canceled", "0", "canceled"),
    ("canceled", "0.5", "canceled"),
    ("expired", "0", "canceled"),
    ("rejected", "0", "rejected"),
]


@pytest.mark.parametrize(("raw", "filled", "ours"), STATUS_TABLE)  # every 3.4 row
def test_status_mapping(raw: str, filled: str, ours: str) -> None:
    assert order_state(raw, Decimal(filled)) == ours


@pytest.mark.parametrize(
    ("raw", "filled"),
    [
        ("filled", "0"),
        ("partially_filled", "0"),
        ("filled", "-1"),
        ("partially_filled", "-0.5"),
        *[(raw, "-0.000000001") for raw in dict.fromkeys(raw for raw, _, _ in STATUS_TABLE)],
    ],
)
def test_a_fill_status_without_a_positive_fill_is_a_broker_error(raw: str, filled: str) -> None:
    # Codex P2-2 (PR #43): `filled` with `filled_qty` 0 would settle the row with nothing
    # booked. The status is known, the pair contradicts itself: a failed read, not
    # `UnknownOrderStatus`. A negative fill fails for every status.
    with pytest.raises(BrokerError) as e:
        order_state(raw, Decimal(filled), "rid-9")
    assert type(e.value) is BrokerError and "(request rid-9)" in str(e.value)
    assert filled not in str(e.value)


@pytest.mark.parametrize("call", ["get_order", "by_client_id", "submit"])
async def test_order_reads_and_acks_with_filled_and_no_fill_are_broker_errors(call: str) -> None:
    body = {**probe("E-final")["json"], "status": "filled", "filled_qty": "0"}
    if call == "submit":
        body = {**ENTRY["json"], "status": "filled", "filled_qty": "0"}
    if call == "get_order":
        body["id"] = OID
    r = Router()
    method, path = _ROUTES[call]
    r.add(method, path, 200, body)
    with pytest.raises(BrokerError) as e:
        await _invoke(await broker(r), call)
    assert type(e.value) is BrokerError  # not UnknownOrderStatus: the read failed
    assert str(e.value).count("(request rid-1)") == 1  # `_parsing` does not append it again
    assert e.value.__cause__ is None and e.value.__suppress_context__
    assert r.unrouted == []


@pytest.mark.parametrize(
    "raw",
    [
        "done_for_day",
        "replaced",
        "pending_replace",
        "stopped",
        "suspended",
        "calculated",
        "held",
        "accepted_for_bidding",
        "brand_new",
    ],
)
def test_unlisted_statuses_raise(raw: str) -> None:
    with pytest.raises(UnknownOrderStatus) as e:
        order_state(raw, Decimal(0), "rid-9")
    assert e.value.status == raw and "rid-9" in str(e.value)


@pytest.mark.parametrize("call", ["get_order", "by_client_id"])
async def test_order_reads_with_an_unlisted_status_raise_unknown_order_status(call: str) -> None:
    r = Router()
    method, path = _ROUTES[call]
    r.add(method, path, 200, {**probe("D2-resting", run="round2")["json"], "status": "held"})
    with pytest.raises(UnknownOrderStatus) as e:
        await _invoke(await broker(r), call)
    assert e.value.status == "held" and "rid-1" in str(e.value)
    assert e.value.__cause__ is None and e.value.__suppress_context__
    assert r.unrouted == []


@pytest.mark.parametrize("call", ["get_order", "by_client_id", "submit"])
async def test_an_off_pattern_status_is_withheld_from_the_exception(call: str) -> None:
    # Controller ruling: the status in the message and in `.status` (which the engine logs)
    # goes through `diag`; the raw value only drives the mapping.
    raw = f"held{KEY}{SECRET}" + "x" * 100
    body = {**probe("D2-resting", run="round2")["json"], "status": raw}
    if call == "submit":
        body = {**ENTRY["json"], "status": raw}
    r = Router()
    method, path = _ROUTES[call]
    r.add(method, path, 200, body)
    with pytest.raises(UnknownOrderStatus) as e:
        await _invoke(await broker(r), call)
    text = f"{e.value} {e.value.status}"
    assert raw not in text and KEY not in text and SECRET not in text
    assert e.value.status == f"<withheld: {len(raw)} chars>"
    assert "(request rid-1)" in str(e.value)


# --- Review Focus 1-5 --------------------------------------------------------------------


async def test_a_json_number_where_a_decimal_belongs_is_a_broker_error() -> None:  # RF 1
    resting = probe("D2-resting", run="round2")["json"]
    r = Router()
    r.add("GET", f"/v2/orders/{resting['id']}", 200, {**resting, "filled_qty": 0.5})
    r.add("GET", "/v2/account", 200, {**ACCOUNT, "equity": 100000.55})
    r.add("POST", "/v2/orders", 200, {**ENTRY["json"], "qty": 0.000204026})
    b = await broker(r)
    with pytest.raises(BrokerError, match="rid-1"):
        await b.get_order(resting["id"])
    with pytest.raises(BrokerError, match="rid-2"):
        await b.get_account()
    with pytest.raises(BrokerError, match="rid-3") as e:
        await b.submit_order(order_from(ENTRY["body"]))
    assert type(e.value) is BrokerError
    assert r.unrouted == []


async def test_a_2xx_html_body_is_a_broker_error() -> None:  # RF 2
    r = Router()
    r.add("GET", "/v2/positions", 200, content=HTML)
    r.add("POST", "/v2/orders", 200, content=HTML)
    r.add("GET", f"/v2/orders/{OID}", 200, content=HTML)
    b = await broker(r)
    with pytest.raises(BrokerError):
        await b.get_positions()
    with pytest.raises(BrokerError) as e:
        await b.submit_order(order_from(ENTRY["body"]))
    assert type(e.value) is BrokerError  # the engine records the row unknown
    with pytest.raises(BrokerError, match="rid-3"):
        await b.get_order(OID)
    assert r.unrouted == []


async def test_nanosecond_z_timestamps_parse_and_naive_ones_fail() -> None:  # RF 3
    r = Router()
    r.add("POST", "/v2/orders", 200, ENTRY["json"])  # submitted_at 2026-11-12T10:26:03.148318561Z
    naive = {**ENTRY["json"], "submitted_at": "2026-11-12T10:26:03.148318561"}
    r.add("POST", "/v2/orders", 200, naive)
    no_submitted = {**ENTRY["json"], "submitted_at": None}
    r.add("POST", "/v2/orders", 200, no_submitted)
    b = await broker(r)
    ack = await b.submit_order(order_from(ENTRY["body"]))
    assert ack.submitted_at == datetime(2026, 11, 12, 10, 26, 3, 148318, tzinfo=UTC)
    with pytest.raises(BrokerError, match="rid-2"):
        await b.submit_order(order_from(ENTRY["body"]))
    ack = await b.submit_order(order_from(ENTRY["body"]))  # falls back to created_at
    assert ack.submitted_at == datetime(2026, 11, 12, 10, 26, 3, 148318, tzinfo=UTC)
    assert r.unrouted == []


async def test_ack_symbol_is_normalised() -> None:  # RF 4
    r = Router()
    r.add("POST", "/v2/orders", 200, {**ENTRY["json"], "symbol": "BTCUSD"})
    r.add("POST", "/v2/orders", 200, {**ENTRY["json"], "symbol": "WEIRD"})
    b = await broker(r)
    ack = await b.submit_order(order_from(ENTRY["body"]))
    assert ack.symbol == "BTC/USD"
    with pytest.raises(BrokerError) as e:
        await b.submit_order(order_from(ENTRY["body"]))
    assert type(e.value) is BrokerError and "WEIRD" not in str(e.value)
    assert "(request rid-2)" in str(e.value)
    assert r.unrouted == []


async def test_ack_with_unlisted_status_is_unknown() -> None:  # RF 5
    ioc = probe("C2-ioc-nofill", run="round2")
    r = Router()
    r.add("POST", "/v2/orders", 200, {**ENTRY["json"], "status": "held"})
    r.add("POST", "/v2/orders", 200, {**ioc["json"], "status": "canceled"})
    b = await broker(r)
    with capture_logs() as logs, pytest.raises(UnknownOrderStatus):
        await b.submit_order(order_from(ENTRY["body"]))
    # final review M2: the order is live at the broker; the operator needs its id
    (line,) = [x for x in logs if x["event"] == "alpaca.ack_status_unknown"]
    assert line["log_level"] == "error"
    assert (line["order_id"], line["status"], line["request_id"]) == (
        ENTRY["json"]["id"],
        "held",
        "rid-1",
    )
    ack = await b.submit_order(order_from(ioc["body"]))
    assert ack.status == "canceled"
    assert r.unrouted == []


async def test_an_unknown_status_ack_logs_no_secret() -> None:  # final review M2
    r = Router()
    raw = f"held{SECRET}"
    r.add("POST", "/v2/orders", 200, {**ENTRY["json"], "status": raw})
    t = structlog.get_logger("t")
    buf = io.StringIO()
    configure_logging(stream=buf)  # production chain; tests/conftest.py restores it
    with pytest.raises(UnknownOrderStatus):
        try:
            await (await broker(r)).submit_order(order_from(ENTRY["body"]))
        except BrokerError:
            t.exception("risk.order_unknown")
            raise
    rendered = buf.getvalue()
    assert "alpaca.ack_status_unknown" in rendered and ENTRY["json"]["id"] in rendered
    assert SECRET not in rendered and KEY not in rendered and raw not in rendered


async def test_a_malformed_status_or_ack_leaks_no_secret_into_the_rendered_log() -> None:
    # Codex r2/r3: the rendered production log and str(exc) carry neither key.
    resting = probe("D2-resting", run="round2")["json"]
    position = {
        "symbol": f"XYZ{SECRET}",
        "asset_class": "crypto",
        "qty": "1",
        "qty_available": "1",
        "avg_entry_price": "1",
    }
    r = Router()
    r.add("GET", f"/v2/orders/{OID}", 200, {**resting, "status": f"held{SECRET}"})
    r.add("POST", "/v2/orders", 200, {**ENTRY["json"], "symbol": f"BTC/USD{SECRET}"})
    r.add("GET", "/v2/positions", 200, [position])
    b = await broker(r)
    t = structlog.get_logger("t")

    async def case_1() -> None:
        try:
            await b.get_order(OID)
        except UnknownOrderStatus as e:  # the engine's line (risk/engine.py `_read`)
            t.warning("risk.order_status_unknown", status=e.status)
            raise

    async def case_2() -> None:
        await b.submit_order(order_from(ENTRY["body"]))

    async def case_3() -> None:
        await b.get_positions()

    for case, marker in ((case_1, "<withheld"), (case_2, "[redacted]"), (case_3, "<withheld")):
        buf = io.StringIO()
        configure_logging(stream=buf)  # production chain; tests/conftest.py restores it
        with pytest.raises(BrokerError) as e:
            try:
                await case()
            except BrokerError:
                t.exception("risk.order_unknown")  # renders the traceback, as the engine does
                raise
        rendered, text = buf.getvalue(), str(e.value)
        assert SECRET not in rendered and KEY not in rendered
        assert SECRET not in text and KEY not in text
        # Case 2: "BTC/USD" + the secret fits FIELD_RE, so diag shows it with the secret
        # redacted; the other two cases fail their pattern and are withheld.
        assert marker in rendered or marker in text
    assert r.unrouted == []


# --- connect and startup cleanup (acceptance 15, adapter part) ---------------------------


async def test_connect_closes_the_clients_when_validation_fails_or_is_cancelled(
    recorded: list[AlpacaHttp],
) -> None:
    # (a) crypto trading inactive
    with pytest.raises(BrokerError, match="crypto trading not active"):
        await connect(account_router({**ACCOUNT, "crypto_status": "INACTIVE"}))
    # (b) the account check blocks; the connect task is cancelled after the request started
    started, never = asyncio.Event(), asyncio.Event()

    async def blocking(request: httpx.Request) -> httpx.Response:
        started.set()
        await never.wait()
        return httpx.Response(200, json=ACCOUNT)

    task = asyncio.create_task(
        AlpacaBroker.connect(settings(), transport=httpx.MockTransport(blocking))
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(recorded) == 2
    assert all(h.closed for h in recorded)


async def test_satisfies_broker_protocol() -> None:
    b: Broker = await connect(account_router())  # mypy checks the structural match
    assert b.mode() == "paper"
    await b.aclose()


async def test_account_values_are_exact_decimals() -> None:
    r = account_router()
    r.add("GET", "/v2/account", 200, ACCOUNT)
    b = await connect(r)
    account = await b.get_account()
    assert account.equity == Decimal("100000.55")
    assert account.cash == Decimal("99000.10")
    assert r.unrouted == []
    await b.aclose()


async def test_fee_schedule_follows_account_crypto_tier() -> None:
    tier1 = await connect(account_router())
    assert tier1.fee_schedule() == FeeSchedule(
        maker_bps=Decimal(15), taker_bps=Decimal(25), buy_fee_asset="base"
    )
    tier3 = await connect(account_router({**ACCOUNT, "crypto_tier": 3}))
    assert tier3.fee_schedule() == FEE_TIERS[3]
    await tier1.aclose()
    await tier3.aclose()


async def test_unknown_crypto_tier_refuses_to_guess_fees(recorded: list[AlpacaHttp]) -> None:
    with pytest.raises(BrokerError, match="crypto_tier 99"):
        await connect(account_router({**ACCOUNT, "crypto_tier": 99}))
    assert [h.closed for h in recorded] == [True]


async def test_inactive_crypto_account_is_refused(recorded: list[AlpacaHttp]) -> None:
    with pytest.raises(BrokerError, match="crypto trading not active on this account: INACTIVE"):
        await connect(account_router({**ACCOUNT, "crypto_status": "INACTIVE"}))
    assert [h.closed for h in recorded] == [True]


async def test_connect_withholds_an_off_pattern_account_value() -> None:  # Codex r2
    weird = f"NOPE {SECRET}"
    with pytest.raises(BrokerError) as e:
        await connect(account_router({**ACCOUNT, "crypto_status": weird}))
    assert SECRET not in str(e.value) and f"<withheld: {len(weird)} chars>" in str(e.value)
    assert "(request rid-1)" in str(e.value)


async def test_live_mode_requires_allow_live(recorded: list[AlpacaHttp]) -> None:
    with pytest.raises(BrokerError, match="ALLOW_LIVE"):
        await connect(Router(), alpaca_paper=False)
    assert recorded == []  # no client is built
    live = await connect(account_router(), alpaca_paper=False, allow_live=True)
    assert live.mode() == "live" and live.can_place_orders() is False
    assert str(recorded[0]._read.base_url) == f"{LIVE_URL}/"
    await live.aclose()


async def test_connect_without_keys_is_a_broker_error(recorded: list[AlpacaHttp]) -> None:
    no_keys = Settings(_env_file=None, alpaca_api_key_id=None, alpaca_api_secret_key=None)
    with pytest.raises(BrokerError, match="ALPACA_API_KEY_ID"):
        await AlpacaBroker.connect(no_keys)
    assert recorded == []  # no client is built


# --- reads ---------------------------------------------------------------------------------


async def test_asset_rules_from_raw_strings() -> None:
    r = Router()
    r.add("GET", "/v2/assets/BTC/USD", 200, probe("0-asset")["json"])
    rules = await (await broker(r)).asset_rules("BTC/USD")
    assert rules == AssetRules(
        min_qty=Decimal("0.00001357"),
        qty_increment=Decimal("0.000000001"),
        min_notional=Decimal(0),  # Alpaca states no minimum notional (only a $200k max)
        price_increment=Decimal("0.000000001"),
    )
    assert r.unrouted == []


async def test_unknown_asset_is_a_broker_error() -> None:
    r = Router()
    r.add("GET", "/v2/assets/DOGE/USD", 404, {"message": "asset not found"})
    with pytest.raises(BrokerError, match="DOGE/USD"):
        await (await broker(r)).asset_rules("DOGE/USD")
    assert r.unrouted == []


@pytest.mark.parametrize("raw_symbol", ["BTC/USD", "BTCUSD"])
async def test_crypto_positions_use_slash_symbols(raw_symbol: str) -> None:
    positions = [
        {
            "symbol": raw_symbol,
            "asset_class": "crypto",
            "qty": "0.012345678",
            "qty_available": "0.012",
            "avg_entry_price": "61234.5",
        },
        {"symbol": "AAPL", "asset_class": "us_equity", "qty": "1", "avg_entry_price": "200"},
    ]
    r = Router()
    r.add("GET", "/v2/positions", 200, positions)
    (position,) = await (await broker(r)).get_positions()
    assert position.symbol == "BTC/USD"
    assert position.qty == Decimal("0.012345678")
    assert position.qty_available == Decimal("0.012")
    assert position.avg_entry_price == Decimal("61234.5")
    assert r.unrouted == []


def test_fee_tiers_match_published_schedule() -> None:
    # docs.alpaca.markets/docs/crypto-trading, verified 2026-10-11 (see docs/alpaca-notes.md)
    assert {t: (f.maker_bps, f.taker_bps) for t, f in FEE_TIERS.items()} == {
        1: (Decimal(15), Decimal(25)),
        2: (Decimal(12), Decimal(22)),
        3: (Decimal(10), Decimal(20)),
        4: (Decimal(8), Decimal(18)),
        5: (Decimal(5), Decimal(15)),
        6: (Decimal(2), Decimal(13)),
        7: (Decimal(2), Decimal(12)),
        8: (Decimal(0), Decimal(10)),
    }


async def test_unmappable_position_symbol_is_a_broker_error() -> None:
    positions = [
        {
            "symbol": "WEIRD",
            "asset_class": "crypto",
            "qty": "1",
            "qty_available": "1",
            "avg_entry_price": "1",
        }
    ]
    r = Router()
    r.add("GET", "/v2/positions", 200, positions)
    with pytest.raises(BrokerError) as e:
        await (await broker(r)).get_positions()
    text = str(e.value)  # Codex r3: the raw symbol stays out of the message
    assert "WEIRD" not in text and "<withheld: 5 chars>" in text and "(request rid-1)" in text
    assert r.unrouted == []


@pytest.mark.parametrize("raw_symbol", ["../account", "BTC/USD/../x", "BTC/USD?a=1", f"{KEY}/USD"])
async def test_an_unsafe_position_symbol_is_withheld_and_never_reaches_a_path(
    raw_symbol: str,
) -> None:
    """Fable review M1: a position symbol is response data; it goes into `/assets/{symbol}`
    and exception text only after matching the config's `BASE/QUOTE` pattern."""
    positions = [
        {
            "symbol": raw_symbol,
            "asset_class": "crypto",
            "qty": "1",
            "qty_available": "1",
            "avg_entry_price": "1",
        }
    ]
    r = Router()
    r.add("GET", "/v2/positions", 200, positions)
    with pytest.raises(BrokerError) as e:
        await (await broker(r)).get_positions()
    text = str(e.value)
    assert f"<withheld: {len(raw_symbol)} chars>" in text and "(request rid-1)" in text
    assert "account" not in text and KEY not in text and "?" not in text
    assert e.value.__cause__ is None and e.value.__suppress_context__
    assert r.unrouted == []


@pytest.mark.parametrize("symbol", ["../account", "BTC/USD/../x", "btc/usd", "", f"{KEY}/USD"])
async def test_asset_rules_refuses_an_unsafe_symbol_before_any_request(symbol: str) -> None:
    r = Router()  # no route: any request would be recorded as unrouted
    with pytest.raises(BrokerError) as e:
        await (await broker(r)).asset_rules(symbol)
    text = str(e.value)
    assert f"<withheld: {len(symbol)} chars>" in text
    assert (symbol == "" or symbol not in text) and KEY not in text
    assert e.value.__cause__ is None and e.value.__suppress_context__
    assert r.unrouted == [] and r.requests == []


async def test_the_client_id_lookup_withholds_an_unsafe_client_id_and_symbol() -> None:
    """Fable review M2: the view's client id and symbol reach `repair link`'s refusal text
    and its printed `broker_view`. Off-pattern values are withheld, not raised: the order
    exists under our client id, so the caller must see a mismatch (`client_id_mismatch`),
    not a failed lookup (`lookup_failed`)."""
    bad_cid, bad_symbol = f"at-0123abcd-c1-e {KEY}", "../account"
    r = Router()
    body = {**probe("E-visible")["json"], "client_order_id": bad_cid, "symbol": bad_symbol}
    r.add("GET", "/v2/orders:by_client_order_id", 200, body)
    view = await (await broker(r)).get_order_by_client_id("at-0123abcd-c1-e")
    assert view is not None
    assert view.client_order_id == f"<withheld: {len(bad_cid)} chars>"
    assert view.symbol == f"<withheld: {len(bad_symbol)} chars>"
    assert KEY not in view.model_dump_json() and "account" not in view.model_dump_json()
    assert r.unrouted == []


# --- one error contract at the adapter boundary (PR #9 review, ported) ---------------------

TRANSPORT_ERRORS: list[tuple[type[httpx.TransportError], type[BrokerError]]] = [
    (httpx.ConnectError, RequestNotSent),
    (httpx.ReadTimeout, BrokerError),
    (httpx.RemoteProtocolError, BrokerError),
]


def failing(path: str, exc: type[httpx.TransportError]) -> Router:
    r = Router()
    r.add("GET", path, raises=exc)
    return r


@pytest.mark.parametrize(("exc", "raised"), TRANSPORT_ERRORS)
async def test_connect_failures_are_broker_errors(
    exc: type[httpx.TransportError], raised: type[BrokerError], recorded: list[AlpacaHttp]
) -> None:
    with pytest.raises(BrokerError) as e:
        await connect(failing("/v2/account", exc))
    assert type(e.value) is raised and exc.__name__ in str(e.value)
    assert e.value.__cause__ is None  # no cause chain in a rendered traceback (Codex P2-1)
    assert [h.closed for h in recorded] == [True]


@pytest.mark.parametrize(("exc", "raised"), TRANSPORT_ERRORS)
async def test_account_read_failures_are_broker_errors(
    exc: type[httpx.TransportError], raised: type[BrokerError]
) -> None:
    r = failing("/v2/account", exc)
    with pytest.raises(BrokerError) as e:
        await (await broker(r)).get_account()
    assert type(e.value) is raised and exc.__name__ in str(e.value)
    assert e.value.__cause__ is None and r.unrouted == []


@pytest.mark.parametrize(("exc", "raised"), TRANSPORT_ERRORS)
async def test_position_read_failures_are_broker_errors(
    exc: type[httpx.TransportError], raised: type[BrokerError]
) -> None:
    r = failing("/v2/positions", exc)
    with pytest.raises(BrokerError) as e:
        await (await broker(r)).get_positions()
    assert type(e.value) is raised and exc.__name__ in str(e.value)
    assert e.value.__cause__ is None and r.unrouted == []


async def test_error_messages_do_not_echo_sdk_messages() -> None:
    # The transport error text can carry request details; only the type name is surfaced.
    marker = f"https://user:pass@paper-api/... {SECRET}"

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError(marker, request=request)

    with pytest.raises(BrokerError) as e:
        await AlpacaBroker.connect(settings(), transport=httpx.MockTransport(handler))
    assert "pass" not in str(e.value) and SECRET not in str(e.value)
    assert "ReadError" in str(e.value)


@pytest.mark.parametrize(
    "account",
    [
        {k: v for k, v in ACCOUNT.items() if k != "equity"},
        {**ACCOUNT, "cash": "not-a-number"},
        [ACCOUNT],
    ],
)
async def test_malformed_account_payload_is_a_broker_error(account: Any) -> None:
    r = Router()
    r.add("GET", "/v2/account", 200, account)
    with pytest.raises(BrokerError, match="unexpected") as e:
        await (await broker(r)).get_account()
    assert "(request rid-1)" in str(e.value) and e.value.__cause__ is None
    assert r.unrouted == []


async def test_malformed_position_payload_is_a_broker_error() -> None:
    positions = [
        {"symbol": "BTC/USD", "asset_class": "crypto", "qty": "1", "qty_available": "1"}
    ]  # no avg_entry_price
    r = Router()
    r.add("GET", "/v2/positions", 200, positions)
    with pytest.raises(BrokerError, match="unexpected"):
        await (await broker(r)).get_positions()
    assert r.unrouted == []


async def test_an_empty_position_list_reads_as_no_holdings() -> None:
    r = Router()
    r.add("GET", "/v2/positions", 200, [])
    assert await (await broker(r)).get_positions() == []
    assert r.unrouted == []


_BTC_ROW = {"symbol": "BTC/USD", "qty": "1", "qty_available": "1", "avg_entry_price": "1"}


@pytest.mark.parametrize(
    "body",
    [
        {},
        "",
        {"positions": []},
        [f"row {SECRET}"],
        [None],
        [_BTC_ROW],  # no asset_class
        [{**_BTC_ROW, "asset_class": None}],
        [{**_BTC_ROW, "asset_class": 7}],
        [{**_BTC_ROW, "asset_class": "crypto"}, {**_BTC_ROW, "asset_class": ["crypto"]}],
    ],
)
async def test_a_positions_body_that_is_not_a_list_of_classed_rows_is_a_broker_error(
    body: Any,
) -> None:
    # Codex P2-1 (PR #43): an empty holdings list is a claim that the account is flat (the
    # Position Manager's flatten check reads it so); a `200 {}` or a row without a string
    # `asset_class` must fail the read, not read as "no crypto holdings".
    r = Router()
    r.add("GET", "/v2/positions", 200, body)
    with pytest.raises(BrokerError) as e:
        await (await broker(r)).get_positions()
    text = str(e.value)
    assert type(e.value) is BrokerError and "(request rid-1)" in text
    assert SECRET not in text and "BTC" not in text
    assert e.value.__cause__ is None and e.value.__suppress_context__
    assert r.unrouted == []


async def test_cancellation_is_not_swallowed() -> None:
    started, never = asyncio.Event(), asyncio.Event()

    async def blocking(request: httpx.Request) -> httpx.Response:
        started.set()
        await never.wait()
        return httpx.Response(200, json={})

    http = AlpacaHttp(
        base_url=PAPER_URL, key_id=KEY, secret_key=SECRET, transport=httpx.MockTransport(blocking)
    )
    task = asyncio.create_task(AlpacaBroker(http, paper=True, fees=FEE_TIERS[1]).get_account())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await http.aclose()


async def test_order_fills_is_not_implemented_until_wp_1_6b() -> None:
    b = await broker(Router())
    with pytest.raises(OrderMethodsUnavailable, match=r"until WP 1\.6b"):
        await b.order_fills("any")


def test_broker_order_is_the_wire_order() -> None:
    # order_from mirrors the probe body (guards the helper the tests above rely on)
    assert order_from(ENTRY["body"]) == BrokerOrder(
        symbol="BTC/USD",
        side="buy",
        type="limit",
        qty=Decimal("0.000204026"),
        limit_price=Decimal("73667.30052"),
        time_in_force="ioc",
        client_order_id="probe16a-20261112T102600-btc-E-entry",
    )
