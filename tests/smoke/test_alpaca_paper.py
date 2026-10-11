"""Read-only smoke tests against the Alpaca paper API. Opt-in: `uv run pytest -m smoke`.

Needs ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY (paper) in env or `.env`. Places no orders.
"""

from decimal import Decimal

import pytest

from autotrader.broker.alpaca import AlpacaBroker
from autotrader.core.config import Settings

pytestmark = pytest.mark.smoke

UNIVERSE = ("BTC/USD", "ETH/USD", "SOL/USD")


@pytest.fixture
async def broker() -> AlpacaBroker:
    settings = Settings()
    if settings.alpaca_api_key_id is None or settings.alpaca_api_secret_key is None:
        pytest.skip("Alpaca paper keys not configured")
    if not settings.alpaca_paper:
        pytest.fail("smoke tests run against paper only; set ALPACA_PAPER=true")
    return await AlpacaBroker.connect(settings)


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
