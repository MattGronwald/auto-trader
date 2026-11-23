"""Read-only smoke tests against the Alpaca paper API. Opt-in: `uv run pytest -m smoke`.

Needs ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY (paper) in env or `.env`. Places no orders:
the order tests only look up ids that do not exist.
"""

from collections.abc import AsyncIterator
from decimal import Decimal
from uuid import uuid4

import pytest

from autotrader.broker.alpaca import AlpacaBroker
from autotrader.broker.base import OrderNotFound
from autotrader.core.config import Settings

pytestmark = pytest.mark.smoke

UNIVERSE = ("BTC/USD", "ETH/USD", "SOL/USD")


@pytest.fixture
async def broker() -> AsyncIterator[AlpacaBroker]:
    settings = Settings()
    if settings.alpaca_api_key_id is None or settings.alpaca_api_secret_key is None:
        pytest.skip("Alpaca paper keys not configured")
    if not settings.alpaca_paper:
        pytest.fail("smoke tests run against paper only; set ALPACA_PAPER=true")
    connected = await AlpacaBroker.connect(settings)
    try:
        yield connected
    finally:  # unclosed clients fail the run on ResourceWarning
        await connected.aclose()


async def test_paper_account_is_readable(broker: AlpacaBroker) -> None:
    account = await broker.get_account()

    assert broker.mode() == "paper"
    assert account.equity >= 0
    assert account.cash >= 0


async def test_positions_are_readable(broker: AlpacaBroker) -> None:
    positions = await broker.get_positions()

    assert all("/" in p.symbol for p in positions)


@pytest.mark.parametrize("symbol", UNIVERSE)
async def test_universe_asset_rules(broker: AlpacaBroker, symbol: str) -> None:
    rules = await broker.asset_rules(symbol)

    assert rules.min_qty > 0
    assert rules.qty_increment > 0
    assert rules.min_qty % rules.qty_increment == Decimal(0)


async def test_unknown_order_id_is_not_found(broker: AlpacaBroker) -> None:
    with pytest.raises(OrderNotFound):
        await broker.get_order(str(uuid4()))


async def test_unknown_client_id_lookup_is_none(broker: AlpacaBroker) -> None:
    assert await broker.get_order_by_client_id(f"smoke-{uuid4()}") is None
