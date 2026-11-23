"""Acceptance 2 and 5 end to end: `RiskEngine` with the real `AlpacaBroker` over
`httpx.MockTransport` (WP 1.6a spec 3.10). Catches contract gaps between the engine's
handling (Task 3) and the adapter's mapping (Task 5) that each side's own stubs cannot.
"""

from collections.abc import AsyncIterator
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import httpx
import pytest
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
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
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
