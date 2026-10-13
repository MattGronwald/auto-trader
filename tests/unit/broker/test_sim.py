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
    Fill,
    OrderNotFound,
    OrderRejected,
)
from autotrader.broker.sim import FillPlan, SimBroker
from autotrader.core.types import Bar

T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
BTC = "BTC/USD"
RULES = {
    BTC: AssetRules(
        min_qty=Decimal("0.0001"),
        qty_increment=Decimal("0.0001"),
        min_notional=Decimal("1"),
        price_increment=Decimal("0.01"),
    ),
    "ETH/USD": AssetRules(
        min_qty=Decimal("0.001"),
        qty_increment=Decimal("0.001"),
        min_notional=Decimal("1"),
        price_increment=Decimal("0.01"),
    ),
}
FEES = FeeSchedule(maker_bps=Decimal("15"), taker_bps=Decimal("25"), buy_fee_asset="quote")


def test_sim_refuses_coin_denominated_buy_fees_until_it_models_them() -> None:
    # SimBroker charges every fee in USD (until WP 1.R2). Accepting a "base" schedule
    # would make the Risk Engine size for coin fees the simulator never applies.
    with pytest.raises(ValueError, match="buy_fee_asset"):
        make_sim(fees=FEES.model_copy(update={"buy_fee_asset": "base"}))


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
    sim = make_sim(
        fees=FeeSchedule(maker_bps=Decimal(0), taker_bps=Decimal(0), buy_fee_asset="quote")
    )
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
    sim = make_sim(
        fees=FeeSchedule(maker_bps=Decimal(0), taker_bps=Decimal(0), buy_fee_asset="quote")
    )
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


async def test_fills_carry_stable_unique_ids_and_fee_asset() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    a = await sim.submit_order(market("buy", "1"))
    b = await sim.submit_order(market("buy", "2"))
    sim.process_bar(bar(1, 100.0))

    stream = sim.stream_fills()
    fills = [await asyncio.wait_for(anext(stream), timeout=1) for _ in range(2)]

    assert [f.fill_id for f in fills] == [f"{a.order_id}:1", f"{b.order_id}:1"]
    assert all(f.fee_asset == "quote" for f in fills)


async def test_redeliver_repeats_the_identical_fill() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    stream = sim.stream_fills()
    first = await asyncio.wait_for(anext(stream), timeout=1)

    sim.redeliver(first.fill_id)

    assert await asyncio.wait_for(anext(stream), timeout=1) == first
    with pytest.raises(KeyError):
        sim.redeliver("sim-999:1")


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
            min_qty=Decimal("0.1"),
            qty_increment=Decimal("0.01"),
            min_notional=Decimal(0),
            price_increment=Decimal("0.01"),
        )
    }
    sim = SimBroker(cash=Decimal(1000), fees=FEES, slippage_bps=Decimal(0), asset_rules=rules)
    sim.process_bar(bar(0, 150.0, symbol="SOL/USD"))

    with pytest.raises(OrderRejected, match="minimum quantity"):
        await sim.submit_order(market("buy", "0.05", symbol="SOL/USD"))
    with pytest.raises(OrderRejected, match="unknown symbol"):
        await sim.asset_rules("DOGE/USD")


# --- regressions from the PR #8 review -------------------------------------------


@pytest.mark.parametrize("replay_minute", [0, -1])  # same bar again, or an older one
async def test_replayed_or_stale_bar_never_fills_new_orders(replay_minute: int) -> None:
    # Look-ahead: re-processing the bar the submitter already saw filled at its open.
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    ack = await sim.submit_order(market("buy", "1"))

    processed = sim.process_bar(bar(replay_minute, 80.0, c=80.0))

    assert processed is False
    assert (await sim.get_order(ack.order_id)).status == "new"
    assert (await sim.get_account()).equity == Decimal("1000")  # stale close not used
    assert sim.process_bar(bar(1, 101.0)) is True
    assert (await sim.get_order(ack.order_id)).avg_fill_price == Decimal("101")


async def test_bar_progression_is_tracked_per_symbol() -> None:
    sim = make_sim()
    sim.process_bar(bar(5, 100.0))
    # An ETH bar at an earlier minute is new for ETH and must be processed.
    assert sim.process_bar(bar(1, 10.0, symbol="ETH/USD")) is True


def test_other_timeframes_are_refused() -> None:
    sim = make_sim()
    five_min = Bar(symbol=BTC, timeframe="5m", ts=T0, open=1, high=1, low=1, close=1, volume=1)

    with pytest.raises(ValueError, match="timeframe"):
        sim.process_bar(five_min)


@pytest.mark.parametrize("field", ["maker_bps", "taker_bps"])
@pytest.mark.parametrize("value", [Decimal(-1), Decimal(10_000)])
def test_fee_bps_must_stay_below_100_percent(field: str, value: Decimal) -> None:
    # D16 sizing assumes 0 <= fee < 1; at >= 100 % the net stop loss could turn <= 0.
    kwargs = {"maker_bps": Decimal(15), "taker_bps": Decimal(25), field: value}
    with pytest.raises(ValueError, match=field):
        FeeSchedule(**kwargs, buy_fee_asset="quote")


@pytest.mark.parametrize("value", [0.1 + 0.2, 1.0, True])
def test_broker_models_reject_floats_and_bools(value: object) -> None:
    with pytest.raises(ValueError, match="Decimal"):
        BrokerOrder(symbol=BTC, side="buy", qty=value, type="market")
    with pytest.raises(ValueError, match="Decimal"):
        FeeSchedule(maker_bps=value, taker_bps=Decimal(1), buy_fee_asset="quote")


@pytest.mark.parametrize("value", [Decimal("0.3"), "0.3", 3])
def test_broker_models_accept_exact_inputs(value: object) -> None:
    order = BrokerOrder(symbol=BTC, side="buy", qty=value, type="market")

    assert order.qty == Decimal(str(value))


async def test_close_of_fully_reserved_position_is_a_typed_rejection() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    await sim.submit_order(limit("sell", "1", "200"))

    with pytest.raises(OrderRejected, match="fully reserved"):
        await sim.close_position(BTC)


async def test_repeated_close_is_a_typed_rejection() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    await sim.close_position(BTC)

    with pytest.raises(OrderRejected, match="fully reserved"):
        await sim.close_position(BTC)


async def test_close_all_continues_past_a_rejected_symbol() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    sim.process_bar(bar(0, 10.0, symbol="ETH/USD"))
    await sim.submit_order(market("buy", "1"))
    await sim.submit_order(market("buy", "2", symbol="ETH/USD"))
    sim.process_bar(bar(1, 100.0))
    sim.process_bar(bar(1, 10.0, symbol="ETH/USD"))
    await sim.submit_order(limit("sell", "1", "500"))  # BTC fully reserved

    acks = await sim.close_all()

    assert [a.symbol for a in acks] == ["ETH/USD"]


async def test_order_never_fills_on_a_bar_before_its_submission() -> None:
    # PR #8 re-review: the stale-bar guard is per symbol, but submission time is the
    # global clock. BTC 00:00, ETH 00:05, submit BTC (acked 00:05), then BTC 00:01
    # filled the order four minutes before it existed.
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    sim.process_bar(bar(5, 10.0, symbol="ETH/USD"))
    ack = await sim.submit_order(market("buy", "1"))
    assert ack.submitted_at == T0 + timedelta(minutes=5)

    sim.process_bar(bar(1, 90.0))  # new for BTC, but before the submission
    assert (await sim.get_order(ack.order_id)).status == "new"

    sim.process_bar(bar(6, 101.0))
    status = await sim.get_order(ack.order_id)
    assert status.status == "filled"
    assert status.avg_fill_price == Decimal("101")


async def test_clock_never_moves_backwards() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(5, 100.0))
    sim.process_bar(bar(1, 10.0, symbol="ETH/USD"))  # a lagging symbol

    ack = await sim.submit_order(market("buy", "1"))

    assert ack.submitted_at == T0 + timedelta(minutes=5)


def ioc_buy(qty: str, limit: str, symbol: str = BTC) -> BrokerOrder:
    return BrokerOrder(
        symbol=symbol,
        side="buy",
        qty=Decimal(qty),
        type="limit",
        limit_price=Decimal(limit),
        time_in_force="ioc",
    )


async def test_ioc_limit_fills_through_limit_with_taker_fee() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100))
    ack = await sim.submit_order(ioc_buy("1", "100.1"))
    sim.process_bar(bar(1, 100.05, h=100.2, low=99.9))
    status = await sim.get_order(ack.order_id)
    assert status.status == "filled"
    assert status.avg_fill_price == Decimal("100.05")  # better of open and limit
    fill = await anext(sim.stream_fills())
    assert fill.fee == Decimal("100.05") * Decimal("25") / Decimal(10_000)  # taker


async def test_ioc_limit_not_reached_on_next_bar_is_canceled() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100))
    ack = await sim.submit_order(ioc_buy("1", "100.1"))
    sim.process_bar(bar(1, 101, h=101.5, low=100.5))
    assert (await sim.get_order(ack.order_id)).status == "canceled"
    sim.process_bar(bar(2, 99, h=99, low=98))  # would have filled a gtc order
    assert (await sim.get_order(ack.order_id)).status == "canceled"
    assert sim.cash == Decimal("1000")


async def test_ioc_cash_reservation_uses_taker_fee() -> None:
    sim = make_sim(cash="100.35")  # 100.1 * 1.0025 = 100.35025 > cash
    sim.process_bar(bar(0, 100))
    with pytest.raises(OrderRejected, match="insufficient cash"):
        await sim.submit_order(ioc_buy("1", "100.1"))


# --- scripted fills (WP 1.R2) ---------------------------------------------------------

D = Decimal


async def drain(sim: SimBroker) -> list[Fill]:
    stream = sim.stream_fills()
    out: list[Fill] = []
    while True:
        try:
            out.append(await asyncio.wait_for(anext(stream), timeout=0.01))
        except TimeoutError:
            return out


def test_fill_plan_rejects_invalid_steps() -> None:
    for bad in (D(0), D("1.01"), D("-0.5"), D("NaN"), D("Infinity"), "later"):
        with pytest.raises(ValueError, match="step"):
            FillPlan((bad,))  # type: ignore[arg-type]
    assert FillPlan((D("0.5"), D(1), "cancel", "reject")).steps[-1] == "reject"


async def test_ioc_partial_fill_cancels_the_rest() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.4"),)))
    ack = await sim.submit_order(ioc_buy("1", "101"))
    sim.process_bar(bar(1, 100.0))

    status = await sim.get_order(ack.order_id)
    assert (status.status, status.filled_qty, status.avg_fill_price) == (
        "canceled",
        D("0.4"),
        D("100"),
    )
    (pos,) = await sim.get_positions()
    assert pos.qty == D("0.4")
    (fill,) = await drain(sim)
    assert (fill.fill_id, fill.order_id, fill.qty) == (f"{ack.order_id}:1", ack.order_id, D("0.4"))


async def test_ioc_scripted_cancel_on_its_bar() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan(("cancel",)))
    ack = await sim.submit_order(ioc_buy("1", "101"))
    sim.process_bar(bar(1, 100.0))

    status = await sim.get_order(ack.order_id)
    assert (status.status, status.filled_qty, status.avg_fill_price) == ("canceled", D(0), None)
    assert await drain(sim) == []
    assert sim.cash == D("1000")


async def test_multi_bar_market_sell_goes_partial_then_filled() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    await drain(sim)
    sim.script(BTC, "sell", FillPlan((D("0.5"), D("0.3"))))
    ack = await sim.submit_order(market("sell", "1"))

    states = []
    for i, price in ((2, 110.0), (3, 120.0), (4, 130.0)):
        sim.process_bar(bar(i, price))
        states.append((await sim.get_order(ack.order_id)).status)

    assert states == ["partially_filled", "partially_filled", "filled"]
    status = await sim.get_order(ack.order_id)
    assert status.filled_qty == D(1)
    assert status.avg_fill_price == D("117")  # 0.5*110 + 0.3*120 + 0.2*130
    fills = await drain(sim)
    assert [f.fill_id for f in fills] == [f"{ack.order_id}:{n}" for n in (1, 2, 3)]
    assert [f.qty for f in fills] == [D("0.5"), D("0.3"), D("0.2")]
    assert await sim.get_positions() == []


@pytest.mark.parametrize(
    ("last", "status", "reason"),
    [("reject", "rejected", "scripted rejection"), ("cancel", "canceled", None)],
)
async def test_scripted_end_keeps_earlier_fills(last: str, status: str, reason: str | None) -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"), last)))  # type: ignore[arg-type]
    ack = await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    sim.process_bar(bar(2, 100.0))
    sim.process_bar(bar(3, 100.0))  # nothing more happens after a final state

    result = await sim.get_order(ack.order_id)
    assert (result.status, result.filled_qty, result.reason) == (status, D("0.5"), reason)
    (pos,) = await sim.get_positions()
    assert pos.qty == D("0.5")


async def test_non_crossing_limit_bar_consumes_no_step() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    ack = await sim.submit_order(limit("buy", "1", "90"))

    sim.process_bar(bar(1, 100.0, low=95.0))  # does not trade through 90
    assert (await sim.get_order(ack.order_id)).status == "new"
    sim.process_bar(bar(2, 100.0, low=89.0))
    assert (await sim.get_order(ack.order_id)).filled_qty == D("0.5")
    sim.process_bar(bar(3, 100.0, low=89.0))
    assert (await sim.get_order(ack.order_id)).status == "filled"


async def test_scripts_match_fifo_per_symbol_and_side() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.4"),)))
    sim.script(BTC, "buy", FillPlan((D("0.6"),)))
    sim.script(BTC, "sell", FillPlan(("reject",)))  # never consumed by a buy
    a = await sim.submit_order(ioc_buy("1", "101"))
    b = await sim.submit_order(ioc_buy("1", "101"))
    c = await sim.submit_order(ioc_buy("1", "101"))  # unscripted: fills fully
    sim.process_bar(bar(1, 100.0))

    filled = [(await sim.get_order(x.order_id)).filled_qty for x in (a, b, c)]
    assert filled == [D("0.4"), D("0.6"), D("1")]


async def test_partial_sell_blocks_close_until_canceled() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    sim.script(BTC, "sell", FillPlan((D("0.5"),)))
    sell = await sim.submit_order(limit("sell", "1", "100"))  # gtc: rest stays open
    sim.process_bar(bar(2, 100.0, h=101.0))
    assert (await sim.get_order(sell.order_id)).status == "partially_filled"

    with pytest.raises(OrderRejected, match="fully reserved"):
        await sim.close_position(BTC)  # held 0.5, open sell remainder 0.5
    await sim.cancel_order(sell.order_id)
    ack = await sim.close_position(BTC)
    sim.process_bar(bar(3, 100.0))

    assert (await sim.get_order(ack.order_id)).filled_qty == D("0.5")
    assert (await sim.get_order(sell.order_id)).filled_qty == D("0.5")
    assert await sim.get_positions() == []


async def test_partial_buy_cancel_releases_cash() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    first = await sim.submit_order(limit("buy", "8", "100"))  # gtc
    sim.process_bar(bar(1, 100.0))  # 4 filled: cash 1000 - 400 - 0.6 maker fee
    assert (await sim.get_order(first.order_id)).status == "partially_filled"

    with pytest.raises(OrderRejected, match="insufficient cash"):
        await sim.submit_order(limit("buy", "4", "100"))  # remainder 4 is still reserved
    await sim.cancel_order(first.order_id)
    await sim.submit_order(limit("buy", "4", "100"))

    status = await sim.get_order(first.order_id)
    assert (status.status, status.filled_qty) == ("canceled", D("4"))


async def test_later_slice_failing_the_cash_check_rejects_and_keeps_earlier_fills() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    ack = await sim.submit_order(market("buy", "9"))
    sim.process_bar(bar(1, 100.0))  # 4.5 filled for 451.125
    sim.process_bar(bar(2, 200.0))  # remainder 4.5 at 200 needs 902.25 > 548.875

    status = await sim.get_order(ack.order_id)
    assert (status.status, status.filled_qty, status.reason) == (
        "rejected",
        D("4.5"),
        "insufficient cash at fill",
    )
    assert sim.cash == D("1000") - D("450") * D("1.0025")


async def test_pre_cancel_fill_is_delivered_after_cancel() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    ack = await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    await sim.cancel_order(ack.order_id)  # nobody has read the stream yet

    (fill,) = await drain(sim)  # delayed delivery: executed before the cancel
    assert (fill.order_id, fill.qty) == (ack.order_id, D("0.5"))
    sim.process_bar(bar(2, 100.0))
    assert await drain(sim) == []  # the cancel is final: no later execution
