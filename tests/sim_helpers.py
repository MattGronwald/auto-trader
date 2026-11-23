"""Test helpers for SimBroker."""

from decimal import Decimal

from autotrader.broker.base import BrokerOrder, OrderAck, OrderRejected
from autotrader.broker.sim import SimBroker


async def sell_free(sim: SimBroker, symbol: str) -> OrderAck:
    """Submit a market sell of the position's free qty (`qty_available`), as `exit()` does
    since D20. No position or nothing free raises `OrderRejected`, as `close_position` did."""
    held = next((p for p in await sim.get_positions() if p.symbol == symbol), None)
    if held is None:
        raise OrderRejected(f"no position in {symbol}")
    if held.qty_available <= Decimal(0):
        raise OrderRejected(f"position in {symbol} is already fully reserved by pending sells")
    return await sim.submit_order(
        BrokerOrder(symbol=symbol, side="sell", qty=held.qty_available, type="market")
    )
