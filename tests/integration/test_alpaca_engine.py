"""Acceptance 2 and 5 end to end: `RiskEngine` with the real `AlpacaBroker` over
`httpx.MockTransport` (WP 1.6a spec 3.10). Catches contract gaps between the engine's
handling (Task 3) and the adapter's mapping (Task 5) that each side's own stubs cannot.
"""

import io
from collections.abc import AsyncIterator
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import tests.unit.risk.test_engine as te
import tests.unit.risk.test_resolver as tr
from autotrader.broker.alpaca import FEE_TIERS, AlpacaBroker
from autotrader.broker.alpaca_http import PAPER_URL, AlpacaHttp
from autotrader.broker.base import (
    BrokerError,
    BrokerOrder,
    OrderAck,
    OrderRejected,
    RequestNotSent,
)
from autotrader.core.bus import EventBus
from autotrader.core.kill import ControlGate
from autotrader.core.ledger import Ledger
from autotrader.core.logging import configure_logging
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.journal.models import Order
from autotrader.risk.resolve import entry_client_id
from autotrader.risk.types import ExitRequest
from tests.unit.broker.alpaca_mock import KEY, SECRET, Router, probe

BTC, NOW = "BTC/USD", te.NOW
SEC = tr.SEC


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)  # runs its own event loop: a sync fixture
    return url


@pytest.fixture
async def sessions(db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = make_engine(db_url)
    yield make_sessionmaker(engine)
    await engine.dispose()


@pytest.fixture
def gate(tmp_path: Path, sessions: async_sessionmaker[AsyncSession]) -> ControlGate:
    return ControlGate(tmp_path / "KILL", sessions)


class Spy(AlpacaBroker):
    """Records what `submit_order` raised: the engine turns any `Exception` into `unknown`,
    so without this a mis-keyed route or a parse bug would pass the `unknown` rows."""

    raised: list[BaseException]

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        try:
            return await super().submit_order(order)
        except BaseException as e:
            self.raised.append(e)
            raise


def spy(router: Router) -> Spy:
    http = AlpacaHttp(base_url=PAPER_URL, key_id=KEY, secret_key=SECRET, transport=router.transport)
    broker = Spy(http, paper=True, fees=FEE_TIERS[1], orders_enabled=True)
    broker.raised = []
    return broker


Outcome = tuple[Any, ...]  # ("status", code, body) or ("raise", httpx exception class)


@pytest.mark.parametrize(
    ("outcome", "row_status", "cause"),
    [
        (("status", 422, probe("F-duplicate-cid")["json"]), "unknown", BrokerError),
        (("status", 403, probe("B-below-min")["json"]), "rejected", OrderRejected),
        (("raise", httpx.ConnectError), "not_submitted", RequestNotSent),
        (("raise", httpx.ConnectTimeout), "not_submitted", RequestNotSent),
        (("raise", httpx.PoolTimeout), "not_submitted", RequestNotSent),
        (("raise", httpx.WriteError), "unknown", BrokerError),
        (("raise", httpx.ReadTimeout), "unknown", BrokerError),
        (("raise", httpx.RemoteProtocolError), "unknown", BrokerError),
    ],
)
async def test_submit_outcomes_through_the_engine(
    outcome: Outcome,
    row_status: str,
    cause: type[BrokerError],
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
) -> None:
    router = Router()
    router.add("GET", "/v2/positions", 200, probe("H-pos-before")["json"])  # 0.000203515 BTC
    router.add("GET", "/v2/assets/BTC/USD", 200, probe("0-asset")["json"])
    if outcome[0] == "status":
        router.add("POST", "/v2/orders", outcome[1], outcome[2])
    else:
        router.add("POST", "/v2/orders", raises=outcome[1])
    broker = spy(router)
    try:
        engine = te.make_engine_(broker, sessions, gate)
        result = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))  # needs no feed
        (row,) = [o for o in await te.orders(sessions) if o.side == "sell"]
        assert (result.status, row.status) == (row_status, row_status)
        assert [type(e) for e in broker.raised] == [cause]  # exact class, not a subclass
        assert [r.method for r in router.requests if r.url.path == "/v2/orders"] == ["POST"]
        assert router.unrouted == []
    finally:
        await broker.aclose()


async def test_an_unknown_status_row_becomes_an_unknown_status_incident(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    held = {**probe("D2-resting", run="round2")["json"], "status": "held"}
    oid = held["id"]
    cid = entry_client_id(await tr.jid_of(sessions), 1)
    row = await tr.insert_row(
        sessions,
        cid=cid,
        status="new",
        broker_order_id=oid,
        type_="limit",
        limit_price=D(held["limit_price"]),
    )
    router = Router()
    router.add("GET", f"/v2/orders/{oid}", 200, held, repeat=True)
    router.add("GET", "/v2/orders:by_client_order_id", 200, held, repeat=True)
    broker = spy(router)
    try:
        clock = te.Clock(NOW)
        ledger = Ledger(EventBus(sessions), sessions, clock=clock)
        engine = te.make_engine_(broker, sessions, gate, clock=clock, ledger=ledger)
        first = await engine.sync_orders()
        await ledger.apply_all(first)
        assert row in first.failed
        clock.now = NOW + 81 * SEC  # clearly past order_resolution_timeout_s (80 s)
        await ledger.apply_all(await engine.sync_orders())
        assert (await tr.order(sessions, row)).incident == "unknown_status"
        assert router.unrouted == []
    finally:
        await broker.aclose()


async def test_a_filled_status_with_no_fill_leaves_the_row_unsettled(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    # Codex P2-2 (PR #43): `filled` with `filled_qty` "0" contradicts itself; the read fails
    # (`BrokerError`, not `UnknownOrderStatus`), so the ledger books nothing and the row stays
    # open, instead of settling a filled order with no fill.
    final = {**probe("E-final")["json"], "filled_qty": "0"}
    oid = final["id"]
    cid = entry_client_id(await tr.jid_of(sessions), 1)
    row = await tr.insert_row(
        sessions,
        cid=cid,
        status="new",
        broker_order_id=oid,
        type_="limit",
        limit_price=D(final["limit_price"]),
    )
    router = Router()
    router.add("GET", f"/v2/orders/{oid}", 200, final, repeat=True)
    router.add("GET", "/v2/assets/BTC/USD", 200, probe("0-asset")["json"], repeat=True)
    broker = spy(router)
    try:
        clock = te.Clock(NOW)
        ledger = Ledger(EventBus(sessions), sessions, clock=clock)
        engine = te.make_engine_(broker, sessions, gate, clock=clock, ledger=ledger)
        synced = await engine.sync_orders()
        assert row in synced.failed and not synced.complete
        assert [u for u in synced.updates if u.order_row_id == row] == []
        await ledger.apply_all(synced)
        stored = await tr.order(sessions, row)
        assert (stored.status, stored.settled_at, stored.incident) == ("new", None, None)
        assert router.unrouted == []
    finally:
        await broker.aclose()


async def test_a_filled_status_short_of_its_qty_leaves_a_partly_booked_row_unsettled(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    # Codex round 3 (PR #43): qty 0.01 with 0.005 already booked. A `filled` read reporting
    # 0.005 asked for no executions (nothing beyond the booked qty) and settled the row, so
    # later passes could never book the rest. The read now fails and the row stays open.
    final = {**probe("E-final")["json"], "qty": "0.01", "filled_qty": "0.005"}
    oid = final["id"]
    cid = entry_client_id(await tr.jid_of(sessions), 1)
    row = await tr.insert_row(
        sessions,
        cid=cid,
        status="partially_filled",
        qty=D("0.01"),
        broker_order_id=oid,
        type_="limit",
        limit_price=D(final["limit_price"]),
    )
    async with sessions() as s:
        await s.execute(update(Order).where(Order.id == row).values(filled_qty=D("0.005")))
        await s.commit()
    router = Router()
    router.add("GET", f"/v2/orders/{oid}", 200, final, repeat=True)
    router.add("GET", "/v2/assets/BTC/USD", 200, probe("0-asset")["json"], repeat=True)
    broker = spy(router)
    try:
        clock = te.Clock(NOW)
        ledger = Ledger(EventBus(sessions), sessions, clock=clock)
        engine = te.make_engine_(broker, sessions, gate, clock=clock, ledger=ledger)
        synced = await engine.sync_orders()
        assert row in synced.failed and not synced.complete
        await ledger.apply_all(synced)
        stored = await tr.order(sessions, row)
        assert (stored.status, stored.settled_at) == ("partially_filled", None)
        assert stored.filled_qty == D("0.005")
        assert router.unrouted == []
    finally:
        await broker.aclose()


async def test_a_key_bearing_id_from_the_client_id_lookup_is_never_linked_or_logged(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    # Codex P2-3 (PR #43): the lookup's order id is linked into the journal and later logged.
    # The order otherwise matches the row, so before the UUID check it was linked; one that
    # embeds the key id is now refused, the row stays unlinked, the lookup counts as failed,
    # and the rendered production log carries neither credential.
    cid = entry_client_id(await tr.jid_of(sessions), 1)
    visible = {**probe("E-visible")["json"], "client_order_id": cid, "id": f"{KEY}-0000"}
    row = await tr.insert_row(
        sessions,
        cid=cid,
        status="unknown",
        qty=D(visible["qty"]),
        type_="limit",
        limit_price=D(visible["limit_price"]),
    )
    router = Router()
    router.add("GET", "/v2/orders:by_client_order_id", 200, visible, repeat=True)
    broker = spy(router)
    buf = io.StringIO()
    configure_logging(stream=buf)  # production chain; tests/conftest.py restores it
    try:
        clock = te.Clock(NOW)
        ledger = Ledger(EventBus(sessions), sessions, clock=clock)
        engine = te.make_engine_(broker, sessions, gate, clock=clock, ledger=ledger)
        for seconds in (0, 81):  # within the window, then past order_resolution_timeout_s
            clock.now = NOW + seconds * SEC
            await ledger.apply_all(await engine.sync_orders())
        stored = await tr.order(sessions, row)
        assert (stored.broker_order_id, stored.incident) == (None, "lookup_failed")
        rendered = buf.getvalue()
        assert "risk.order_lookup_failed" in rendered
        assert KEY not in rendered and SECRET not in rendered
        assert [r.url.path for r in router.requests] == ["/v2/orders:by_client_order_id"] * 2
        assert router.unrouted == []
    finally:
        await broker.aclose()
