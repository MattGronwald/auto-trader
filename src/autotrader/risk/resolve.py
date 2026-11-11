"""Pure helpers of the order resolver (spec 2026-11-10): client-id formats, ownership and
the broker-view match. No I/O, no clock. Imports only broker types, so `risk/types.py` can
import `IncidentReason` from here.

Entry ids are `at-{jid}-c{cycle_id}-e`, exit ids `at-{jid}-o{row_id}`; `jid` is the
per-journal id, so ids of another journal (or the legacy `at-c7-e` form) are never ours.
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

from autotrader.broker.base import BrokerOrderView

IncidentReason = Literal[
    "no_ownership_proof", "client_id_mismatch", "not_found", "lookup_failed", "lookup_inconsistent"
]

# The broker's creation time may trail our submit stamp by clock skew; older than this
# before the submit is a different order.
CREATED_AT_SKEW = timedelta(minutes=5)


def entry_client_id(journal_id: str, cycle_id: int) -> str:
    return f"at-{journal_id}-c{cycle_id}-e"


def exit_client_id(journal_id: str, row_id: int) -> str:
    return f"at-{journal_id}-o{row_id}"


def owns(client_order_id: str | None, journal_id: str) -> bool:
    """True iff the id carries this journal's prefix (`at-{jid}-`, the dash ends the jid)."""
    return client_order_id is not None and client_order_id.startswith(f"at-{journal_id}-")


def mismatches(
    view: BrokerOrderView,
    *,
    symbol: str,
    side: str,
    qty: Decimal,
    submitted_at: datetime,
    broker_order_id: str | None,
) -> tuple[str, ...]:
    """One line per field where the broker's order differs from our row; empty = match.
    Ownership (`owns`, exact client-id equality) is the caller's check, not this one's."""
    out: list[str] = []
    if view.symbol != symbol:
        out.append(f"symbol {view.symbol} != {symbol}")
    if view.side != side:
        out.append(f"side {view.side} != {side}")
    if view.qty != qty:
        out.append(f"qty {view.qty} != {qty}")
    if view.created_at < submitted_at - CREATED_AT_SKEW:
        out.append(
            f"created_at {view.created_at.isoformat()} before submit {submitted_at.isoformat()}"
        )
    if broker_order_id is not None and view.order_id != broker_order_id:
        out.append(f"order_id {view.order_id} != {broker_order_id}")
    return tuple(out)
