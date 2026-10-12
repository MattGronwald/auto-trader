"""AlpacaBroker against a fake client returning recorded raw API payloads (no network)."""

import asyncio
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest
import requests
from alpaca.common.exceptions import APIError

from autotrader.broker.alpaca import FEE_TIERS, AlpacaBroker
from autotrader.broker.base import AssetRules, Broker, BrokerError, BrokerOrder, FeeSchedule
from autotrader.core.config import Settings

# Shapes recorded from the paper API on 2026-10-11 (raw_data=True: decimals are strings).
ACCOUNT = {
    "status": "ACTIVE",
    "crypto_status": "ACTIVE",
    "currency": "USD",
    "equity": "100000.55",
    "cash": "99000.10",
    "crypto_tier": 1,
}
BTC_ASSET = {
    "symbol": "BTC/USD",
    "class": "crypto",
    "status": "active",
    "tradable": True,
    "min_order_size": "0.000012054",
    "min_trade_increment": "0.000000001",
    "price_increment": "0.000000001",
}


class FakeClient:
    def __init__(
        self, account: dict[str, Any] | None = None, positions: list[dict[str, Any]] | None = None
    ) -> None:
        self.account = account or ACCOUNT
        self.positions = positions or []
        self.calls: list[str] = []

    def get_account(self) -> dict[str, Any]:
        self.calls.append("get_account")
        return self.account

    def get_all_positions(self) -> list[dict[str, Any]]:
        self.calls.append("get_all_positions")
        return self.positions

    def get_asset(self, symbol: str) -> dict[str, Any]:
        self.calls.append(f"get_asset:{symbol}")
        if symbol != "BTC/USD":
            raise LookupError(symbol)
        return BTC_ASSET


async def connect(client: FakeClient, **settings: Any) -> AlpacaBroker:
    return await AlpacaBroker.connect(Settings(_env_file=None, **settings), client=client)


async def test_satisfies_broker_protocol() -> None:
    broker: Broker = await connect(FakeClient())  # mypy checks the structural match

    assert broker.mode() == "paper"


async def test_account_values_are_exact_decimals() -> None:
    account = await (await connect(FakeClient())).get_account()

    assert account.equity == Decimal("100000.55")
    assert account.cash == Decimal("99000.10")


async def test_fee_schedule_follows_account_crypto_tier() -> None:
    tier3 = {**ACCOUNT, "crypto_tier": 3}

    assert (await connect(FakeClient())).fee_schedule() == FeeSchedule(
        maker_bps=Decimal(15), taker_bps=Decimal(25), buy_fee_asset="base"
    )
    assert (await connect(FakeClient(tier3))).fee_schedule() == FEE_TIERS[3]


async def test_unknown_crypto_tier_refuses_to_guess_fees() -> None:
    with pytest.raises(BrokerError, match="crypto_tier"):
        await connect(FakeClient({**ACCOUNT, "crypto_tier": 99}))


async def test_inactive_crypto_account_is_refused() -> None:
    with pytest.raises(BrokerError, match="crypto"):
        await connect(FakeClient({**ACCOUNT, "crypto_status": "INACTIVE"}))


async def test_live_mode_requires_allow_live() -> None:
    with pytest.raises(BrokerError, match="ALLOW_LIVE"):
        await connect(FakeClient(), alpaca_paper=False)

    live = await connect(FakeClient(), alpaca_paper=False, allow_live=True)
    assert live.mode() == "live"


async def test_asset_rules_from_raw_strings() -> None:
    rules = await (await connect(FakeClient())).asset_rules("BTC/USD")

    assert rules == AssetRules(
        min_qty=Decimal("0.000012054"),
        qty_increment=Decimal("0.000000001"),
        min_notional=Decimal(0),  # Alpaca states no minimum notional (only a $200k max)
        price_increment=Decimal("0.000000001"),
    )


async def test_unknown_asset_is_a_broker_error() -> None:
    with pytest.raises(BrokerError, match="DOGE/USD"):
        await (await connect(FakeClient())).asset_rules("DOGE/USD")


@pytest.mark.parametrize("raw_symbol", ["BTC/USD", "BTCUSD"])
async def test_crypto_positions_use_slash_symbols(raw_symbol: str) -> None:
    positions = [
        {
            "symbol": raw_symbol,
            "asset_class": "crypto",
            "qty": "0.012345678",
            "avg_entry_price": "61234.5",
        },
        {"symbol": "AAPL", "asset_class": "us_equity", "qty": "1", "avg_entry_price": "200"},
    ]

    (position,) = await (await connect(FakeClient(positions=positions))).get_positions()

    assert position.symbol == "BTC/USD"
    assert position.qty == Decimal("0.012345678")
    assert position.avg_entry_price == Decimal("61234.5")


async def test_order_methods_are_not_available_until_wp_1_6() -> None:
    broker = await connect(FakeClient())
    order = BrokerOrder(symbol="BTC/USD", side="buy", qty=Decimal("0.001"), type="market")

    for call in (
        broker.submit_order(order),
        broker.cancel_order("x"),
        broker.get_order("x"),
        broker.close_position("BTC/USD"),
        broker.close_all(),
    ):
        with pytest.raises(BrokerError, match=r"WP 1\.6"):
            await call
    with pytest.raises(BrokerError, match=r"WP 1\.6"):
        broker.stream_fills()


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
    positions = [{"symbol": "WEIRD", "asset_class": "crypto", "qty": "1", "avg_entry_price": "1"}]

    with pytest.raises(BrokerError, match="WEIRD"):
        await (await connect(FakeClient(positions=positions))).get_positions()


async def test_connect_without_keys_is_a_broker_error() -> None:
    settings = Settings(_env_file=None, alpaca_api_key_id=None, alpaca_api_secret_key=None)

    with pytest.raises(BrokerError, match="ALPACA_API_KEY_ID"):
        await AlpacaBroker.connect(settings)


# --- regressions from the PR #9 review: one error contract at the adapter boundary ---


class FailingClient(FakeClient):
    """Raises `error` from the named SDK method; others behave like FakeClient."""

    def __init__(self, method: str, error: BaseException) -> None:
        super().__init__()
        self._method, self._error = method, error

    def _maybe_fail(self, method: str) -> None:
        if method == self._method:
            raise self._error

    def get_account(self) -> dict[str, Any]:
        self._maybe_fail("get_account")
        return super().get_account()

    def get_all_positions(self) -> list[dict[str, Any]]:
        self._maybe_fail("get_all_positions")
        return super().get_all_positions()

    def get_asset(self, symbol: str) -> dict[str, Any]:
        self._maybe_fail("get_asset")
        return super().get_asset(symbol)


def _api_error(body: str) -> Exception:
    error: Exception = APIError(body)  # type: ignore[no-untyped-call]  # untyped in alpaca-py
    return error


# Factories: a fresh exception per test, so tracebacks don't accumulate on shared objects.
TRANSPORT_ERRORS: list[Callable[[], Exception]] = [
    lambda: requests.exceptions.ConnectionError("network down"),
    lambda: _api_error('{"code": 40110000, "message": "request is not authorized"}'),
    lambda: _api_error('{"code": 42910000, "message": "rate limit exceeded"}'),
]


@pytest.mark.parametrize("make_error", TRANSPORT_ERRORS, ids=["connection", "auth", "rate-limit"])
async def test_connect_failures_are_broker_errors(make_error: Callable[[], Exception]) -> None:
    error = make_error()
    with pytest.raises(BrokerError) as exc:
        await connect(FailingClient("get_account", error))

    assert exc.value.__cause__ is error


@pytest.mark.parametrize("make_error", TRANSPORT_ERRORS, ids=["connection", "auth", "rate-limit"])
async def test_account_read_failures_are_broker_errors(make_error: Callable[[], Exception]) -> None:
    error = make_error()
    client = FailingClient("none", error)
    broker = await connect(client)
    client._method = "get_account"

    with pytest.raises(BrokerError) as exc:
        await broker.get_account()

    assert exc.value.__cause__ is error


@pytest.mark.parametrize("make_error", TRANSPORT_ERRORS, ids=["connection", "auth", "rate-limit"])
async def test_position_read_failures_are_broker_errors(
    make_error: Callable[[], Exception],
) -> None:
    error = make_error()
    broker = await connect(FailingClient("get_all_positions", error))

    with pytest.raises(BrokerError) as exc:
        await broker.get_positions()

    assert exc.value.__cause__ is error


async def test_error_messages_do_not_echo_sdk_messages() -> None:
    # The SDK message can carry request details; only the type name is surfaced.
    secretish = requests.exceptions.ConnectionError("https://user:pass@paper-api/...")

    with pytest.raises(BrokerError) as exc:
        await connect(FailingClient("get_account", secretish))

    assert "pass" not in str(exc.value)
    assert "ConnectionError" in str(exc.value)


@pytest.mark.parametrize(
    "account",
    [
        {k: v for k, v in ACCOUNT.items() if k != "equity"},
        {**ACCOUNT, "cash": "not-a-number"},
    ],
)
async def test_malformed_account_payload_is_a_broker_error(account: dict[str, Any]) -> None:
    client = FakeClient()
    broker = await connect(client)
    client.account = account

    with pytest.raises(BrokerError, match="unexpected"):
        await broker.get_account()


async def test_malformed_position_payload_is_a_broker_error() -> None:
    positions = [{"symbol": "BTC/USD", "asset_class": "crypto", "qty": "1"}]  # no avg_entry_price

    with pytest.raises(BrokerError, match="unexpected"):
        await (await connect(FakeClient(positions=positions))).get_positions()


async def test_cancellation_is_not_swallowed() -> None:
    with pytest.raises(asyncio.CancelledError):
        await connect(FailingClient("get_account", asyncio.CancelledError()))
