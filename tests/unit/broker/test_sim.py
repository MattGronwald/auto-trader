import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from autotrader.broker.base import (
    AssetRules,
    Broker,
    BrokerOrder,
    FeeSchedule,
    OrderNotFound,
    OrderRejected,
)
from autotrader.broker.sim import SimBroker
from autotrader.core.types import Bar

T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
BTC = "BTC/USD"
RULES = {
    BTC: AssetRules(
        min_qty=Decimal("0.0001"), qty_increment=Decimal("0.0001"), min_notional=Decimal("1")
    ),
    "ETH/USD": AssetRules(
        min_qty=Decimal("0.001"), qty_increment=Decimal("0.001"), min_notional=Decimal("1")
    ),
}
FEES = FeeSchedule(maker_bps=Decimal("15"), taker_bps=Decimal("25"))


def make_sim(cash: str = "1000", slippage_bps: str = "0", fees: FeeSchedule = FEES) -> SimBroker:
    return SimBroker(
        cash=Decimal(cash), fees=fees, slippage_bps=Decimal(slippage_bps), asset_rules=RULES
    )


def bar(
    i: int,
    o: float,
    h: float | None = None,
    low: float | None = None,
    c: float | None = None,
    symbol: str = BTC,
) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe="1m",
        ts=T0 + timedelta(minutes=i),
        open=o,
        high=h if h is not None else o,
        low=low if low is not None else o,
        close=c if c is not None else o,
        volume=1.0,
    )


def market(side: str, qty: str, symbol: str = BTC) -> BrokerOrder:
    return BrokerOrder(symbol=symbol, side=side, qty=Decimal(qty), type="market")


def limit(side: str, qty: str, price: str, symbol: str = BTC) -> BrokerOrder:
    return BrokerOrder(
        symbol=symbol,
        side=side,
        qty=Decimal(qty),
        type="limit",
        limit_price=Decimal(price),
    )


async def test_sim_satisfies_broker_protocol() -> None:
    broker: Broker = make_sim()  # mypy checks the structural match

    assert broker.mode() == "paper"
    assert broker.fee_schedule() == FEES
    assert await broker.asset_rules(BTC) == RULES[BTC]


# --- market orders -------------------------------------------------------------------


async def test_market_buy_fills_at_next_bar_open_with_slippage_and_taker_fee() -> None:
    sim = make_sim(slippage_bps="10")
    sim.process_bar(bar(0, 100.0))  # price known; order arrives after this bar closed
    ack = await sim.submit_order(market("buy", "1"))

    assert (await sim.get_order(ack.order_id)).status == "new"  # not on the bar it saw
    sim.process_bar(bar(1, 200.0, h=210.0, low=190.0, c=205.0))

    status = await sim.get_order(ack.order_id)
    price = Decimal("200") * Decimal("1.001")  # +10 bps slippage on a buy
    fee = price * Decimal("0.0025")  # 25 bps taker
    assert status.status == "filled"
    assert status.filled_qty == Decimal("1")
    assert status.avg_fill_price == price
    account = await sim.get_account()
    assert account.cash == Decimal("1000") - price - fee
    (position,) = await sim.get_positions()
    assert (position.symbol, position.qty, position.avg_entry_price) == (BTC, Decimal("1"), price)


async def test_market_sell_slips_down() -> None:
    sim = make_sim(cash="10000", slippage_bps="10")
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    ack = await sim.submit_order(market("sell", "1"))
    sim.process_bar(bar(2, 100.0))

    assert (await sim.get_order(ack.order_id)).avg_fill_price == Decimal("100") * Decimal("0.999")
    assert await sim.get_positions() == []


async def test_bar_prices_become_exact_decimals() -> None:
    sim = make_sim(fees=FeeSchedule(maker_bps=Decimal(0), taker_bps=Decimal(0)))
    sim.process_bar(bar(0, 0.1))
    ack = await sim.submit_order(market("buy", "10"))
    sim.process_bar(bar(1, 0.1))

    assert (await sim.get_order(ack.order_id)).avg_fill_price == Decimal("0.1")


# --- limit orders --------------------------------------------------------------------


async def test_limit_buy_fills_only_when_low_reaches_limit_with_maker_fee() -> None:
    sim = make_sim(slippage_bps="50")
    sim.process_bar(bar(0, 100.0))
    ack = await sim.submit_order(limit("buy", "1", "95"))

    sim.process_bar(bar(1, 100.0, h=101.0, low=96.0))
    assert (await sim.get_order(ack.order_id)).status == "new"

    sim.process_bar(bar(2, 97.0, h=98.0, low=94.0))
    status = await sim.get_order(ack.order_id)
    assert status.status == "filled"
    assert status.avg_fill_price == Decimal("95")  # at the limit, no slippage
    fee = Decimal("95") * Decimal("0.0015")
    assert (await sim.get_account()).cash == Decimal("1000") - Decimal("95") - fee


async def test_limit_buy_gapping_below_fills_at_open() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    ack = await sim.submit_order(limit("buy", "1", "95"))
    sim.process_bar(bar(1, 90.0, h=92.0, low=89.0))

    assert (await sim.get_order(ack.order_id)).avg_fill_price == Decimal("90")


async def test_limit_sell_fills_when_high_reaches_limit() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    ack = await sim.submit_order(limit("sell", "1", "110"))
    sim.process_bar(bar(2, 105.0, h=109.0, low=104.0))
    assert (await sim.get_order(ack.order_id)).status == "new"
    sim.process_bar(bar(3, 112.0, h=115.0, low=111.0))

    assert (await sim.get_order(ack.order_id)).avg_fill_price == Decimal("112")  # gap up


async def test_bars_of_other_symbols_do_not_fill() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.process_bar(bar(0, 10.0, symbol="ETH/USD"))
    ack = await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 10.0, symbol="ETH/USD"))

    assert (await sim.get_order(ack.order_id)).status == "new"


# --- validation ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("order", "match"),
    [
        (market("buy", "1", symbol="DOGE/USD"), "unknown symbol"),
        (market("buy", "0.00015"), "increment"),
        (market("buy", "0.00005"), "increment|minimum"),
        (market("buy", "0.0001"), "notional"),  # 0.0001 x 100 = 0.01 USD < 1 USD
        (market("buy", "20"), "insufficient cash"),
        (market("sell", "1"), "exceeds position"),  # spot: no shorting
    ],
)
async def test_submit_rejections(order: BrokerOrder, match: str) -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))

    with pytest.raises(OrderRejected, match=match):
        await sim.submit_order(order)


async def test_no_price_yet_is_rejected() -> None:
    with pytest.raises(OrderRejected, match="no price"):
        await make_sim().submit_order(market("buy", "1"))


def test_order_shape_is_validated() -> None:
    with pytest.raises(ValueError, match="limit_price"):
        BrokerOrder(symbol=BTC, side="buy", qty=Decimal(1), type="limit")
    with pytest.raises(ValueError, match="limit_price"):
        BrokerOrder(symbol=BTC, side="buy", qty=Decimal(1), type="market", limit_price=Decimal(1))
    with pytest.raises(ValueError, match="greater than 0"):
        BrokerOrder(symbol=BTC, side="buy", qty=Decimal(0), type="market")
    with pytest.raises(ValueError, match="greater than 0"):
        BrokerOrder(symbol=BTC, side="buy", qty=Decimal(1), type="limit", limit_price=Decimal(0))


async def test_pending_sells_count_against_position() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    await sim.submit_order(limit("sell", "0.6", "200"))

    with pytest.raises(OrderRejected, match="exceeds position"):
        await sim.submit_order(market("sell", "0.5"))


async def test_pending_buys_count_against_cash() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "6"))

    with pytest.raises(OrderRejected, match="insufficient cash"):
        await sim.submit_order(market("buy", "6"))


async def test_gap_up_beyond_cash_rejects_at_fill() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    ack = await sim.submit_order(market("buy", "9"))  # ~900 + fee: fits at 100
    sim.process_bar(bar(1, 150.0))  # 1350 does not

    assert (await sim.get_order(ack.order_id)).status == "rejected"
    assert (await sim.get_account()).cash == Decimal("1000")
    assert await sim.get_positions() == []


# --- cancel / lookup / close ---------------------------------------------------------


async def test_cancel_open_order() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    ack = await sim.submit_order(limit("buy", "1", "50"))

    await sim.cancel_order(ack.order_id)
    sim.process_bar(bar(1, 40.0))

    assert (await sim.get_order(ack.order_id)).status == "canceled"
    assert await sim.get_positions() == []


async def test_cancel_filled_or_unknown_order_raises() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    ack = await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))

    with pytest.raises(OrderRejected, match="not open"):
        await sim.cancel_order(ack.order_id)
    with pytest.raises(OrderNotFound):
        await sim.cancel_order("nope")
    with pytest.raises(OrderNotFound):
        await sim.get_order("nope")


async def test_client_order_id_is_echoed() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    order = BrokerOrder(
        symbol=BTC, side="buy", qty=Decimal(1), type="market", client_order_id="c-142"
    )

    ack = await sim.submit_order(order)

    assert ack.client_order_id == "c-142"
    assert ack.status == "new"


async def test_close_position_and_close_all() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    sim.process_bar(bar(0, 10.0, symbol="ETH/USD"))
    await sim.submit_order(market("buy", "1"))
    await sim.submit_order(market("buy", "2", symbol="ETH/USD"))
    sim.process_bar(bar(1, 100.0))
    sim.process_bar(bar(1, 10.0, symbol="ETH/USD"))

    acks = await sim.close_all()
    sim.process_bar(bar(2, 100.0))
    sim.process_bar(bar(2, 10.0, symbol="ETH/USD"))

    assert sorted(a.symbol for a in acks) == ["BTC/USD", "ETH/USD"]
    assert await sim.get_positions() == []


async def test_close_position_without_position_raises() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))

    with pytest.raises(OrderRejected, match="no position"):
        await sim.close_position(BTC)


# --- account / fills stream ----------------------------------------------------------


async def test_equity_is_marked_to_last_close() -> None:
    sim = make_sim(fees=FeeSchedule(maker_bps=Decimal(0), taker_bps=Decimal(0)))
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "2"))
    sim.process_bar(bar(1, 100.0, h=130.0, low=99.0, c=125.0))

    account = await sim.get_account()

    assert account.cash == Decimal("800")
    assert account.equity == Decimal("800") + 2 * Decimal("125")


async def test_fills_are_streamed_in_order() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    a = await sim.submit_order(market("buy", "1"))
    b = await sim.submit_order(market("buy", "2"))
    sim.process_bar(bar(1, 100.0))

    stream = sim.stream_fills()
    fills = [await asyncio.wait_for(anext(stream), timeout=1) for _ in range(2)]

    assert [f.order_id for f in fills] == [a.order_id, b.order_id]
    assert fills[0].ts == T0 + timedelta(minutes=1)
    assert fills[0].fee == Decimal("100") * Decimal("0.0025")


# --- invariants ----------------------------------------------------------------------

_actions = st.lists(
    st.tuples(
        st.sampled_from(["buy", "sell", "limit_buy", "limit_sell", "bar"]),
        st.integers(min_value=1, max_value=50),  # qty in 0.001 BTC steps... x 0.01
        st.floats(min_value=50, max_value=150, allow_nan=False),
    ),
    max_size=40,
)


@given(_actions)
@settings(max_examples=200, deadline=None)
def test_cash_and_positions_never_go_negative_and_cash_is_conserved(
    actions: list[tuple[str, int, float]],
) -> None:
    async def run() -> None:
        sim = make_sim(slippage_bps="5")
        sim.process_bar(bar(0, 100.0))
        spent = Decimal(0)
        stream = sim.stream_fills()
        for i, (kind, n, price) in enumerate(actions, start=1):
            qty = str(Decimal(n) / 100)
            p = str(round(price, 2))
            try:
                if kind == "bar":
                    sim.process_bar(bar(i, price, h=price * 1.01, low=price * 0.99))
                elif kind in ("buy", "sell"):
                    await sim.submit_order(market(kind, qty))
                else:
                    await sim.submit_order(limit(kind.removeprefix("limit_"), qty, p))
            except OrderRejected:
                pass
            account = await sim.get_account()
            assert account.cash >= 0
            assert all(pos.qty > 0 for pos in await sim.get_positions())
        while True:
            try:
                fill = await asyncio.wait_for(anext(stream), timeout=0.001)
            except TimeoutError:
                break
            signed = fill.qty * fill.price
            spent += (signed if fill.side == "buy" else -signed) + fill.fee
        # Exact up to Decimal's 28-significant-digit rounding (summation order differs
        # between sim and test): ~1e-24 USD at these magnitudes, far below a cent.
        assert abs((await sim.get_account()).cash - (Decimal("1000") - spent)) < Decimal("1e-18")

    asyncio.run(run())


async def test_min_qty_above_increment_and_unknown_asset_rules() -> None:
    rules = {
        "SOL/USD": AssetRules(
            min_qty=Decimal("0.1"), qty_increment=Decimal("0.01"), min_notional=Decimal(0)
        )
    }
    sim = SimBroker(cash=Decimal(1000), fees=FEES, slippage_bps=Decimal(0), asset_rules=rules)
    sim.process_bar(bar(0, 150.0, symbol="SOL/USD"))

    with pytest.raises(OrderRejected, match="minimum quantity"):
        await sim.submit_order(market("buy", "0.05", symbol="SOL/USD"))
    with pytest.raises(OrderRejected, match="unknown symbol"):
        await sim.asset_rules("DOGE/USD")
