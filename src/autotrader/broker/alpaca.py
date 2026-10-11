"""Alpaca adapter (SPEC §4.6) — read side. Order submission and fills land in WP 1.6.

Facts verified 2026-10-11 against docs.alpaca.markets/docs/crypto-trading and the paper
API; see docs/alpaca-notes.md for sources, raw values and open questions.

- The client runs with `raw_data=True`: the API returns decimals as strings, while the
  alpaca-py models turn asset sizes into floats. Strings go straight into `Decimal`.
- alpaca-py is synchronous; calls run in a worker thread (`asyncio.to_thread`).
- Fees are volume-tiered; `fee_schedule()` follows the account's `crypto_tier`.
- No minimum notional is published (only a $200k maximum per order).
"""

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, NoReturn, Protocol

from autotrader.broker.base import (
    Account,
    AssetRules,
    BrokerError,
    BrokerOrder,
    FeeSchedule,
    Fill,
    Mode,
    OrderAck,
    OrderStatus,
    Position,
)
from autotrader.core.config import Settings

# 30-day-volume fee tiers (maker, taker in bps), docs verified 2026-10-11.
FEE_TIERS: dict[int, FeeSchedule] = {
    tier: FeeSchedule(maker_bps=Decimal(maker), taker_bps=Decimal(taker))
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


class RawTradingClient(Protocol):
    """The subset of `alpaca.trading.client.TradingClient(raw_data=True)` used here."""

    def get_account(self) -> Any: ...
    def get_all_positions(self) -> Any: ...
    def get_asset(self, symbol: str) -> Any: ...


def _slash_symbol(raw: str) -> str:
    """Positions may report crypto as `BTCUSD`; the rest of the system uses `BTC/USD`."""
    if "/" in raw:
        return raw
    for quote in ("USD", "USDT", "USDC", "BTC"):
        if raw.endswith(quote) and len(raw) > len(quote):
            return f"{raw[: -len(quote)]}/{quote}"
    raise BrokerError(f"cannot map position symbol {raw!r} to BASE/QUOTE")


class AlpacaBroker:
    def __init__(self, client: RawTradingClient, *, paper: bool, fees: FeeSchedule) -> None:
        self._client = client
        self._paper = paper
        self._fees = fees

    @classmethod
    async def connect(
        cls, settings: Settings, client: RawTradingClient | None = None
    ) -> "AlpacaBroker":
        """Build a broker from settings and check the account can trade crypto.

        `client` is for tests; normally a raw-data `TradingClient` is created from the
        configured keys. Live mode additionally requires ALLOW_LIVE=1 (SPEC §12), on top
        of the same check at config load.
        """
        paper = settings.alpaca_paper
        if not paper and not settings.allow_live:
            raise BrokerError("live Alpaca account requested but ALLOW_LIVE=1 is not set")
        if client is None:
            client = _make_client(settings, paper)
        account = await asyncio.to_thread(client.get_account)
        if str(account.get("crypto_status")).upper() != "ACTIVE":
            raise BrokerError(
                f"crypto trading not active on this account: {account.get('crypto_status')}"
            )
        tier = account.get("crypto_tier")
        if tier not in FEE_TIERS:
            raise BrokerError(f"unknown crypto_tier {tier!r}; update FEE_TIERS from the docs")
        return cls(client, paper=paper, fees=FEE_TIERS[tier])

    # --- read side -------------------------------------------------------------------

    async def get_account(self) -> Account:
        raw = await asyncio.to_thread(self._client.get_account)
        return Account(equity=Decimal(raw["equity"]), cash=Decimal(raw["cash"]))

    async def get_positions(self) -> list[Position]:
        raw = await asyncio.to_thread(self._client.get_all_positions)
        return [
            Position(
                symbol=_slash_symbol(p["symbol"]),
                qty=Decimal(p["qty"]),
                avg_entry_price=Decimal(p["avg_entry_price"]),
            )
            for p in raw
            if p.get("asset_class") == "crypto"
        ]

    async def asset_rules(self, symbol: str) -> AssetRules:
        try:
            raw = await asyncio.to_thread(self._client.get_asset, symbol)
        except Exception as e:
            raise BrokerError(f"cannot load asset {symbol}: {type(e).__name__}") from e
        return AssetRules(
            min_qty=Decimal(raw["min_order_size"]),
            qty_increment=Decimal(raw["min_trade_increment"]),
            min_notional=Decimal(0),
        )

    def fee_schedule(self) -> FeeSchedule:
        return self._fees

    def mode(self) -> Mode:
        return "paper" if self._paper else "live"

    # --- order side: WP 1.6 ----------------------------------------------------------

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        _not_yet("submit_order")

    async def cancel_order(self, order_id: str) -> None:
        _not_yet("cancel_order")

    async def get_order(self, order_id: str) -> OrderStatus:
        _not_yet("get_order")

    async def close_position(self, symbol: str) -> OrderAck:
        _not_yet("close_position")

    async def close_all(self) -> list[OrderAck]:
        _not_yet("close_all")

    def stream_fills(self) -> AsyncIterator[Fill]:
        _not_yet("stream_fills")


def _not_yet(method: str) -> NoReturn:
    raise BrokerError(f"AlpacaBroker.{method} is not implemented until WP 1.6")


def _make_client(settings: Settings, paper: bool) -> RawTradingClient:
    from alpaca.trading.client import TradingClient

    if settings.alpaca_api_key_id is None or settings.alpaca_api_secret_key is None:
        raise BrokerError("ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY are not set")
    client: RawTradingClient = TradingClient(
        settings.alpaca_api_key_id.get_secret_value(),
        settings.alpaca_api_secret_key.get_secret_value(),
        paper=paper,
        raw_data=True,
    )
    return client
