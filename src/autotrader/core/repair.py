"""Journal repair operations behind `autotrader repair` (WP 1.5d-2, spec 2026-11-10 5).

Each operation is two steps. `prepare_*` reads the order row, checks the command's
preconditions and returns a `RepairPlan` (the columns it will change, their current values
and the new ones); any failed precondition raises `RepairRefused` with the reason the CLI
prints. `commit` writes the plan's row change and one `OperatorRepair` audit event in one
`EventBus.publish_tx` unit, and only while every planned column still holds the value the
plan read (conditional `UPDATE`; `not_submitted` also re-checks that the row still has no
broker id); otherwise nothing is written and it raises `RepairRefused`.

- `link` (5.2): records the broker id of an unsettled row that has none (`pending`/`unknown`)
  or that is a type (b) incident, after the broker's order found by the row's own
  journal-namespaced client id proved ownership and matched symbol, side, qty and the
  `created_at` bound. No override: a wrong link books a foreign order.
- `not_submitted` (5.3): marks an unsettled `pending`/`unknown` row without a broker id
  `not_submitted`; the next sync pass settles it with zero fill.
- `oversell` (5.4): flags a sell row whose latest `LedgerAnomaly` is `oversell` as
  `excess_external`; the ledger books the excess as an external holding on the next pass.

Pure journal code (SPEC 0.1): the broker read for `link` arrives as a `Lookup` callable
(`risk.engine.read_order_by_client_id` bound to the CLI's broker). A repair never touches
positions, fills, balances or the manual pause.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast

from sqlalchemy import ColumnElement, CursorResult, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import BrokerOrderView
from autotrader.core.bus import DomainEvent, EventBus
from autotrader.core.ledger import LedgerEvent, verify_unit
from autotrader.journal import repo
from autotrader.journal.models import Order
from autotrader.risk.resolve import mismatches, owns

Action = Literal["link", "not_submitted", "oversell"]
Value = str | bool | None
Lookup = Callable[[str], Awaitable[BrokerOrderView | None]]

# Type (b) incidents (a row with a broker id) that `repair link` may re-link (spec 5.2, D20.11).
LINKABLE_INCIDENTS = frozenset(
    {"not_found", "lookup_failed", "lookup_inconsistent", "client_id_mismatch"}
)
_UNRESOLVED = ("pending", "unknown")
_DATETIME_COLUMNS = frozenset({"incident_at", "resolve_since"})


class RepairRefused(Exception):
    """A precondition failed; the message is the reason the CLI prints."""


@dataclass(frozen=True)
class RepairPlan:
    action: Action
    order_row_id: int
    before: dict[str, Value]  # column -> value read by prepare (datetimes as ISO strings)
    after: dict[str, Value]  # column -> new value; also the event's `after`


class OperatorRepair(LedgerEvent):
    """An operator changed an order row by hand (`autotrader repair`, spec 5.1)."""

    action: Action
    order_row_id: int
    before: dict[str, Any]
    after: dict[str, Any]
    evidence: str
    operator: str


def _value(order: Order, column: str) -> Value:
    raw = getattr(order, column)
    return raw.isoformat() if isinstance(raw, datetime) else cast(Value, raw)


def _plan(order: Order, action: Action, after: dict[str, Value]) -> RepairPlan:
    before = {column: _value(order, column) for column in after}
    return RepairPlan(action=action, order_row_id=order.id, before=before, after=after)


async def _row(sessions: async_sessionmaker[AsyncSession], row_id: int) -> Order:
    async with sessions() as session:
        order = await session.get(Order, row_id, populate_existing=True)
    if order is None:
        raise RepairRefused(f"no order row {row_id}")
    if order.settled_at is not None:
        raise RepairRefused(f"order row {row_id} is already settled")
    return order


async def prepare_link(
    sessions: async_sessionmaker[AsyncSession], row_id: int, broker_order_id: str, lookup: Lookup
) -> RepairPlan:
    order = await _row(sessions, row_id)
    unlinked = order.broker_order_id is None and order.status in _UNRESOLVED
    type_b = order.broker_order_id is not None and order.incident in LINKABLE_INCIDENTS
    if not (unlinked or type_b):
        raise RepairRefused(
            f"order row {row_id} is not linkable: it needs no broker id and status pending/"
            f"unknown, or a broker id and an incident in {sorted(LINKABLE_INCIDENTS)} "
            f"(status {order.status}, broker id {order.broker_order_id}, "
            f"incident {order.incident})"
        )
    async with sessions() as session:
        jid = await repo.journal_id(session)
    cid = order.client_order_id
    if cid is None or jid is None or not owns(cid, jid):
        raise RepairRefused("no ownership proof: use the runbook's manual procedure")
    try:
        view = await lookup(cid)
    except Exception as e:  # a failed broker read refuses; CancelledError propagates
        raise RepairRefused(f"broker lookup failed: {type(e).__name__}: {e}") from e
    if view is None:
        raise RepairRefused(f"the broker has no order with client id {cid}")
    differences = list(
        mismatches(
            view,
            symbol=order.symbol or "",
            side=order.side,
            qty=order.qty,
            submitted_at=order.submitted_at,
            broker_order_id=None,
        )
    )
    if view.client_order_id != cid:
        differences.insert(0, f"client_order_id {view.client_order_id} != {cid}")
    if view.order_id != broker_order_id:
        differences.append(f"order_id {view.order_id} != {broker_order_id}")
    if differences:
        raise RepairRefused("broker order does not match: " + "; ".join(differences))
    return _plan(
        order,
        "link",
        {
            "broker_order_id": broker_order_id,
            "incident": None,
            "incident_at": None,
            "resolve_since": None,
        },
    )


async def prepare_not_submitted(
    sessions: async_sessionmaker[AsyncSession], row_id: int
) -> RepairPlan:
    order = await _row(sessions, row_id)
    if order.broker_order_id is not None or order.status not in _UNRESOLVED:
        raise RepairRefused(
            f"order row {row_id} needs no broker id and status pending/unknown "
            f"(status {order.status}, broker id {order.broker_order_id})"
        )
    return _plan(
        order, "not_submitted", {"status": "not_submitted", "incident": None, "incident_at": None}
    )


async def prepare_oversell(sessions: async_sessionmaker[AsyncSession], row_id: int) -> RepairPlan:
    order = await _row(sessions, row_id)
    if order.side != "sell":
        raise RepairRefused(f"order row {row_id} is a {order.side}, not a sell")
    if order.excess_external:
        raise RepairRefused(f"order row {row_id} is already marked excess_external")
    async with sessions() as session:
        latest = (await repo.last_anomalies(session, [row_id])).get(row_id)
    if latest is None or latest.get("kind") != "oversell":
        kind = None if latest is None else latest.get("kind")
        raise RepairRefused(f"order row {row_id}: latest ledger anomaly is {kind}, not oversell")
    return _plan(order, "oversell", {"excess_external": True})


def _guard(column: str, value: Value) -> ColumnElement[bool]:
    attr = getattr(Order, column)
    if value is None:
        return cast(ColumnElement[bool], attr.is_(None))
    if column in _DATETIME_COLUMNS:
        return cast(ColumnElement[bool], attr == datetime.fromisoformat(cast(str, value)))
    return cast(ColumnElement[bool], attr == value)


async def commit(
    bus: EventBus, plan: RepairPlan, *, evidence: str, operator: str, now: datetime
) -> None:
    """Write `plan` and its `OperatorRepair` event in one unit, or nothing: raises
    `RepairRefused` when the row changed since `prepare_*` read it (or settled)."""
    if not evidence.strip():
        raise RepairRefused("evidence is required")
    refused: list[str] = []
    # `not_submitted` also needs the row still without a broker id: one acked or linked since
    # `prepare_not_submitted` read it was submitted. Not a planned column (no `after` value).
    extra = [Order.broker_order_id.is_(None)] if plan.action == "not_submitted" else []

    async def build(session: AsyncSession) -> DomainEvent | None:
        # A refusal is recorded, not raised: an exception here would be logged as a failed
        # unit (`bus.unit_failed`, error level) for what is a benign outcome.
        result = cast(
            CursorResult[Any],
            await session.execute(
                update(Order)
                .where(
                    Order.id == plan.order_row_id,
                    Order.settled_at.is_(None),
                    *(_guard(column, value) for column, value in plan.before.items()),
                    *extra,
                )
                .values(**plan.after, updated_at=now)
            ),
        )
        if result.rowcount != 1:
            refused.append("row changed since it was read")
            return None
        order = await session.get(Order, plan.order_row_id, populate_existing=True)
        return OperatorRepair(
            ts=now,
            cycle_id=None if order is None else order.cycle_id,
            action=plan.action,
            order_row_id=plan.order_row_id,
            before=plan.before,
            after=plan.after,
            evidence=evidence,
            operator=operator,
        )

    await bus.publish_tx(build, verify_unit)
    if refused:
        raise RepairRefused(refused[0])
