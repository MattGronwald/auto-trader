"""Fill and position accounting (WP 1.5b, D17; spec 2026-10-15-fill-position-accounting-
design.md).

The Risk Engine's order sync reads each unsettled order's broker status and executions;
`Ledger.apply_all()` books them here, one `EventBus.publish_tx` unit per order: the fills
(idempotent by `broker_fill_id`), the order's applied qty, status and settlement, the
position (open, grow, reduce, close) and, when an entry settles, its cycle (D15: any fill
-> FILLED, a final zero fill -> EXPIRED). Pure journal code: the broker stays with the
Risk Engine (SPEC 0.1).

Status and fills are separate broker reads, so the fills may be ahead of the status: fills
are applied as seen, and an order settles only on a final status whose `filled_qty`
equals the applied qty (spec 4.3). Only a contradiction against an authoritative bound is
an anomaly: the update is rolled back and entries are paused until an operator resumes
them (spec 4.5).

A sell marked `external` (1.5c decision 13, spec 4.4.1) of a broker holding the journal
doesn't know is booked without a position: its fills (and so its cash effect) and its
settlement, but no position and no P&L, since there is no buy cost to set against it. On
settlement, once, from the order's booked fills, the sold qty first consumes the symbol's
written-off dust (`symbol_dust`); the remainder is external and reported in
`ExternalHoldingSold`. A marked sell that finds an open position books through the normal
sell path, `oversell` included.

An operator-marked oversell (`repair oversell` sets `orders.excess_external`, WP 1.5d-2,
spec 2026-11-10 5.4) is split per fill, in booking order, when the fill is booked: the
position absorbs up to its open qty plus the carried dust (consumed as for any sell), and
the rest of that fill, and every later fill of the row, is external, persisted in
`fills.external_qty` (0 on every ordinary fill). Position fees and realized P&L take only
each fill's position share; at the row's settlement `ExternalHoldingSold` reports the
external shares from the stored split (`dust_consumed` 0). A marked row whose position
absorbs the whole sell books like any sell, with no external part and no event.

Order resolver decisions (WP 1.5d-2, spec 2026-11-10 3.5-3.7) arrive with the sync pass as
`SyncResult.resolutions`; `apply_all` applies them first, each in its own unit, then books.
Each is a conditional `UPDATE` whose guard lives in its `WHERE` clause (never a Python check
before the write: another session may commit between the two): a link only on a
`pending`/`unknown` row with no broker id, no incident and no settlement (`OrderResolved`); an
incident only once (`OrderIncident`), a type (a) incident only while the row is still
`pending`/`unknown` with no broker id, and a type (b) incident only while `resolve_since`
still holds the clock its decision observed; a clock start never moves an existing clock, and
a clear only erases the clock it observed. A write that matched no row writes nothing and
publishes nothing.

Booking never touches an incident row: `_book` checks `incident` inside its own transaction
and defers, so an update read before the incident committed books nothing and the row keeps
holding back its symbol's later rows until an operator repair clears it (D20.10).

Money columns are text on SQLite, so Decimal values are compared in Python, never in SQL.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, cast
from uuid import uuid4

import structlog
from pydantic import Field
from sqlalchemy import CursorResult, func, select
from sqlalchemy import update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import Account, Fill
from autotrader.core.bus import DomainEvent, EventBus
from autotrader.core.cycle import CycleState, CycleTransitioned, check_edge
from autotrader.core.kill import PAUSED_KEY
from autotrader.journal import repo
from autotrader.journal.models import ControlState, Order, Position, RiskDecision, SymbolDust
from autotrader.journal.models import Event as EventRow
from autotrader.journal.models import Fill as FillRow
from autotrader.risk.types import FINAL, OrderResolution, OrderUpdate, SyncResult

log = structlog.get_logger(__name__)

_RANK = {"new": 1, "partially_filled": 2}  # pending / unknown: 0, final: 3
# Statuses a type (a) resolver write (link, incident) may still find; the same set
# `core/repair.py` accepts for `link` / `not-submitted`.
_TYPE_A_STATUSES = ("pending", "unknown")
_D0 = Decimal(0)
CASH_CHECK_KEY = "cash_check"
CENT = Decimal("0.01")
CashResult = Literal["baseline", "ok", "discrepancy", "skipped"]

AnomalyKind = Literal[
    "fill_mismatch",
    "overfilled",
    "fill_after_settle",
    "sell_without_position",
    "oversell",
    "ambiguous_position",
]
Outcome = Literal["applied", "noop", "deferred", "anomaly"]


class LedgerEvent(DomainEvent):
    """Every ledger event carries the identity of the unit that wrote it: after a commit
    error, `verify_unit` looks for this exact event in the journal (a shared flag such as
    `paused`, or a timestamp, cannot prove that this unit committed). The Position
    Manager's events use the same pattern."""

    unit_id: str = Field(default_factory=lambda: uuid4().hex)


class FillsApplied(LedgerEvent):
    """Fills or a status change of one order were booked; the order is not settled yet."""

    order_row_id: int
    fill_ids: list[str]
    position_id: int | None
    status: str
    filled_qty: Decimal  # the applied qty after this unit
    settled: bool = False


class OrderSettled(FillsApplied):
    """The order's effects are fully applied (spec 4.3): it no longer reserves."""

    settled: bool = True


class DustLeft(LedgerEvent):
    symbol: str
    position_id: int
    dust_qty: Decimal
    min_qty: Decimal


class LedgerAnomaly(LedgerEvent):
    kind: AnomalyKind
    order_row_id: int
    detail: str


class OrderResolved(LedgerEvent):
    """The order resolver linked an unlinked row to its broker order (spec 3.7)."""

    order_row_id: int
    broker_order_id: str
    client_order_id: str | None
    how: Literal["resolver"] = "resolver"


class OrderIncident(LedgerEvent):
    """An unresolved order needs an operator (spec 3.6): the row stays unsettled."""

    order_row_id: int
    symbol: str
    side: str
    qty: Decimal
    client_order_id: str | None
    broker_order_id: str | None
    reason: str
    submitted_at: datetime


class ExternalHoldingSold(LedgerEvent):
    """A settled external sale (spec 4.4.1): no position, no P&L; accounted for by hand."""

    order_row_id: int
    symbol: str
    qty: Decimal
    dust_consumed: Decimal
    external_qty: Decimal
    proceeds_usd: Decimal
    fees_usd: Decimal


class CashChecked(LedgerEvent):
    result: Literal["baseline", "ok"]
    expected: Decimal | None
    actual: Decimal
    n_fills: int


class CashDiscrepancy(LedgerEvent):
    expected: Decimal
    actual: Decimal
    diff: Decimal
    n_fills: int
    baseline_ts: str


@dataclass(frozen=True)
class CashGuard:
    """The order/fill generation and whether any order was unsettled at one read (4.6)."""

    generation: tuple[int, int]
    unsettled: bool


async def _guard(session: AsyncSession) -> CashGuard:
    generation = await repo.order_fill_generation(session)
    open_orders = await session.scalar(
        select(func.count()).select_from(Order).where(Order.settled_at.is_(None))
    )
    return CashGuard(generation, bool(open_orders))


class _Deferred(Exception):
    """Not an error: the update waits for earlier history on its symbol (spec 4.3)."""


class _Anomaly(Exception):
    def __init__(self, kind: AnomalyKind, detail: str) -> None:
        super().__init__(f"{kind}: {detail}")
        self.kind: AnomalyKind = kind
        self.detail = detail


@dataclass
class _Unit:
    """What a unit's `build` reports to its caller (`build` itself only returns events)."""

    error: _Deferred | _Anomaly | None = None
    cycle_id: int | None = None
    result: CashResult = "skipped"
    external: ExternalHoldingSold | None = None


def _rank(status: str) -> int:
    return 3 if status in FINAL else _RANK.get(status, 0)


def _norm(value: Decimal) -> Decimal:
    """Written by value (spec 3): 1.50 and 1.5 are one quantity, stored without exponent."""
    return Decimal(format(value.normalize(), "f"))


def _net_qty(fill: Fill) -> Decimal:
    """Coins a buy delivers: a coin fee comes out of the fill (G10, D10)."""
    return fill.qty - fill.fee if fill.fee_asset == "base" else fill.qty


def _fee_usd(fee: Decimal, fee_asset: str, price: Decimal) -> Decimal:
    return fee * price if fee_asset == "base" else fee


def _cash_effect(row: FillRow) -> Decimal:
    """Cash one fill moved: a coin fee reduces coins, never cash (G10)."""
    notional = row.qty * row.price
    if row.side == "buy":
        return -notional - (row.fee if row.fee_asset == "quote" else _D0)
    return notional - row.fee


def _split(amount: Decimal, qty: Decimal, external_qty: Decimal) -> tuple[Decimal, Decimal]:
    """`(position_part, external_part)` of a fill's amount by its persisted external qty
    (spec 2026-11-10 5.4). An ordinary fill (`external_qty == 0`) returns `amount` itself:
    at 28-digit precision `a * q / q != a` for some values, so the arithmetic would leave a
    rounding residue in ordinary trades. The parts sum to `amount` exactly; any residue
    lands in the external part."""
    if external_qty == 0:
        return amount, _D0
    position_part = amount * (qty - external_qty) / qty
    return position_part, amount - position_part


def _same(row: FillRow, fill: Fill) -> bool:
    return (row.side, row.fee_asset, row.qty, row.price, row.fee) == (
        fill.side,
        fill.fee_asset,
        fill.qty,
        fill.price,
        fill.fee,
    )


async def verify_unit(session: AsyncSession, event: DomainEvent) -> bool:
    """Read-back after a commit error (D15): is this unit's own head event in the journal?
    It commits atomically with the unit's rows, so finding it proves the whole unit."""
    if not isinstance(event, LedgerEvent):
        raise TypeError(f"cannot verify {type(event).__name__}")
    payloads = await session.scalars(
        select(EventRow.payload).where(
            EventRow.type == type(event).__name__, EventRow.ts == event.ts
        )
    )
    return any(p.get("unit_id") == event.unit_id for p in payloads)


async def _entry_adjusted(session: AsyncSession, cycle_id: int | None) -> dict[str, Any]:
    """The approved entry decision's `adjusted` (D16 R denominator, ATR at entry)."""
    if cycle_id is None:
        return {}
    adjusted = await session.scalar(
        select(RiskDecision.adjusted)
        .where(
            RiskDecision.cycle_id == cycle_id,
            RiskDecision.kind == "entry",
            RiskDecision.approved.is_(True),
        )
        .order_by(RiskDecision.id.desc())
        .limit(1)
    )
    return {} if adjusted is None else dict(adjusted)


def _opt_dec(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


async def _earlier_unsettled(session: AsyncSession, order: Order) -> bool:
    n = await session.scalar(
        select(func.count())
        .select_from(Order)
        .where(Order.symbol == order.symbol, Order.id < order.id, Order.settled_at.is_(None))
    )
    return bool(n)


class Ledger:
    def __init__(
        self,
        bus: EventBus,
        sessions: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._bus = bus
        self._sessions = sessions
        self._clock = clock

    # --- orders ------------------------------------------------------------------------

    async def apply_all(self, result: SyncResult) -> frozenset[str]:
        """Book one sync pass (spec 4.1): per symbol in order-row id order. An unsettled row
        without an update (failed lookup, or pending/unknown with no broker id), and an
        update that was deferred or anomalous, hold back the later rows of its symbol, so a
        catch-up never books an exit before its entry.

        A row whose booking raises is logged and holds back its own symbol only; the other
        symbols still book. Returns the symbols that raised on this pass (the caller fails
        closed on them). `CancelledError` is never caught.

        The pass's resolutions are applied first (`apply_resolutions`); a linked row is read
        and booked by a later pass."""
        await self.apply_resolutions(result.resolutions)
        updates = {u.order_row_id: u for u in result.updates}
        async with self._sessions() as session:
            rows = await repo.unsettled_orders(session)
        held: set[str] = set()
        failed: set[str] = set()
        for row in rows:
            symbol = row.symbol or ""
            update = updates.get(row.id)
            if symbol in held:
                if update is not None:
                    log.info("ledger.update_deferred", order_row=row.id, symbol=symbol)
                continue
            if update is None:
                held.add(symbol)
                continue
            try:
                outcome = await self.apply(update)
            except Exception:
                log.exception("ledger.row_failed", order_row=row.id, symbol=symbol)
                held.add(symbol)
                failed.add(symbol)
                continue
            if outcome in ("deferred", "anomaly"):
                held.add(symbol)
        return frozenset(failed)

    async def apply_resolutions(self, resolutions: Sequence[OrderResolution]) -> None:
        """Apply the order resolver's decisions in order, each in its own conditional unit
        (module docstring). One that raises is logged `ledger.resolution_failed` and does
        not stop the others; `CancelledError` propagates."""
        for r in resolutions:
            try:
                await self._resolve(r)
            except Exception:
                log.exception("ledger.resolution_failed", order_row=r.order_row_id, action=r.action)

    async def _resolve(self, r: OrderResolution) -> None:
        if r.action == "link":
            await self._link(r)
        elif r.action == "incident":
            await self._incident(r)
        else:
            if r.action == "start_clock":
                stmt = (
                    sql_update(Order)
                    .where(
                        Order.id == r.order_row_id,
                        Order.resolve_since.is_(None),
                        Order.incident.is_(None),
                        Order.settled_at.is_(None),
                    )
                    .values(resolve_since=r.at)
                )
            else:  # clear_clock: only the clock its read observed
                stmt = (
                    sql_update(Order)
                    .where(Order.id == r.order_row_id, Order.resolve_since == r.clock)
                    .values(resolve_since=None)
                )
            async with self._sessions() as session:
                await session.execute(stmt)
                await session.commit()

    async def _link(self, r: OrderResolution) -> None:
        async def build(session: AsyncSession) -> DomainEvent | None:
            result = cast(
                CursorResult[Any],
                await session.execute(
                    sql_update(Order)
                    .where(
                        Order.id == r.order_row_id,
                        Order.broker_order_id.is_(None),
                        Order.status.in_(_TYPE_A_STATUSES),
                        Order.settled_at.is_(None),
                        Order.incident.is_(None),
                    )
                    .values(broker_order_id=r.broker_order_id, updated_at=r.at)
                ),
            )
            if result.rowcount != 1 or r.broker_order_id is None:
                return None
            order = await self._reread(session, r.order_row_id)
            return OrderResolved(
                ts=r.at,
                cycle_id=order.cycle_id,
                order_row_id=order.id,
                broker_order_id=r.broker_order_id,
                client_order_id=order.client_order_id,
            )

        if await self._bus.publish_tx(build, verify_unit) is not None:
            log.info(
                "risk.order_resolved", order_row=r.order_row_id, broker_order_id=r.broker_order_id
            )

    async def _incident(self, r: OrderResolution) -> None:
        guard = [
            Order.id == r.order_row_id,
            Order.incident.is_(None),
            Order.settled_at.is_(None),
        ]
        if r.clock is not None:  # type (b): only while the clock it observed still holds
            guard.append(Order.resolve_since == r.clock)
        else:  # type (a): only while the row still has no broker id (not acked since the read)
            # and is not locally final since the read (rejected / not_submitted): an incident
            # there could not be repaired, `repair` accepts only pending / unknown rows.
            guard.append(Order.broker_order_id.is_(None))
            guard.append(Order.status.in_(_TYPE_A_STATUSES))

        async def build(session: AsyncSession) -> DomainEvent | None:
            result = cast(
                CursorResult[Any],
                await session.execute(
                    sql_update(Order)
                    .where(*guard)
                    .values(incident=r.reason, incident_at=r.at, resolve_since=None)
                ),
            )
            if result.rowcount != 1 or r.reason is None:
                return None
            order = await self._reread(session, r.order_row_id)
            return OrderIncident(
                ts=r.at,
                cycle_id=order.cycle_id,
                order_row_id=order.id,
                symbol=order.symbol or "",
                side=order.side,
                qty=order.qty,
                client_order_id=order.client_order_id,
                broker_order_id=order.broker_order_id,
                reason=r.reason,
                submitted_at=order.submitted_at,
            )

        event = await self._bus.publish_tx(build, verify_unit)
        if isinstance(event, OrderIncident):
            log.error(
                "risk.order_incident",
                order_row=event.order_row_id,
                symbol=event.symbol,
                reason=event.reason,
            )

    @staticmethod
    async def _reread(session: AsyncSession, row_id: int) -> Order:
        order = await session.get(Order, row_id, populate_existing=True)
        if order is None:
            raise LookupError(f"no order row {row_id}")
        return order

    async def apply(self, update: OrderUpdate) -> Outcome:
        """Book one order's update in one transaction. Never raises for a deferral or an
        anomaly; journal errors propagate to the caller (`apply_all` catches them per row
        and holds back that row's symbol)."""
        unit = _Unit()

        async def build(session: AsyncSession) -> tuple[DomainEvent, ...] | None:
            try:
                return await self._book(session, update, unit)
            except (_Deferred, _Anomaly) as e:
                unit.error = e
                return None

        event = await self._bus.publish_tx(build, verify_unit)
        if unit.external is not None:
            sold = unit.external
            log.warning(
                "positions.external_sold",
                order_row=sold.order_row_id,
                symbol=sold.symbol,
                external_qty=str(sold.external_qty),
                dust_consumed=str(sold.dust_consumed),
            )
        if isinstance(unit.error, _Anomaly):
            await self._anomaly(update.order_row_id, unit.cycle_id, unit.error)
            return "anomaly"
        if isinstance(unit.error, _Deferred):
            log.info("ledger.exit_deferred", order_row=update.order_row_id, reason=str(unit.error))
            return "deferred"
        return "noop" if event is None else "applied"

    async def _book(
        self, session: AsyncSession, u: OrderUpdate, unit: _Unit
    ) -> tuple[DomainEvent, ...] | None:
        now = self._clock()
        order = await session.get(Order, u.order_row_id)
        if order is None:
            raise LookupError(f"no order row {u.order_row_id}")
        unit.cycle_id = order.cycle_id
        if order.incident is not None:
            # Read inside the booking transaction: an update read before the incident
            # committed (check() and the sync loop overlap) must not book it. Only an
            # operator repair clears it (D20.10); deferring holds the symbol's later rows.
            raise _Deferred(f"order row {order.id} is an open incident ({order.incident})")
        booked = {
            r.broker_fill_id: r
            for r in await session.scalars(select(FillRow).where(FillRow.order_id == order.id))
        }
        new: list[Fill] = []
        for fill in sorted(u.fills, key=lambda f: (f.ts, f.fill_id)):
            row = booked.get(fill.fill_id)
            if row is not None:
                if not _same(row, fill):
                    raise _Anomaly(
                        "fill_mismatch", f"fill {fill.fill_id} differs from the booked one"
                    )
            elif all(f.fill_id != fill.fill_id for f in new):
                new.append(fill)
        if order.settled_at is not None:
            if new:
                raise _Anomaly("fill_after_settle", f"new fill {new[0].fill_id} on a settled order")
            return None  # a stale update for an order that is already settled
        applied = order.filled_qty + sum((f.qty for f in new), _D0)
        final = u.status in FINAL
        # An exit sells an exact qty (its free holding, via submit_order), but rows from
        # journals that closed with close_position can exceed their recorded qty: the sell
        # side stays exempt. Only an entry's own qty bounds it.
        if order.side == "buy" and applied > order.qty:
            raise _Anomaly("overfilled", f"applied {applied} > ordered {order.qty}")
        if final and u.filled_qty < applied:
            raise _Anomaly(
                "overfilled", f"final {u.status} reports {u.filled_qty} < applied {applied}"
            )
        position = (
            None if order.position_id is None else await session.get(Position, order.position_id)
        )
        for fill in new:
            fill_row = FillRow(
                order_id=order.id,
                broker_fill_id=fill.fill_id,
                side=fill.side,
                fee_asset=fill.fee_asset,
                qty=_norm(fill.qty),
                price=_norm(fill.price),
                fee=_norm(fill.fee),
                ts=fill.ts,
                external_qty=_D0,
            )
            session.add(fill_row)
            if order.side == "buy":
                position = await self._buy(session, order, position, fill)
            else:
                position = await self._sell(session, order, position, fill, fill_row, now)
        changed = bool(new)
        if new:
            order.filled_qty = _norm(applied)
            if order.side == "buy" and position is not None:
                await self._reprice(session, order, position)
        if _rank(u.status) > _rank(order.status):
            order.status = u.status
            changed = True
        extra: list[DomainEvent] = []
        settled = final and u.filled_qty == applied
        if settled:
            order.settled_at = now
            changed = True
            if order.side == "buy":
                moved = await self._complete_cycle(session, order, applied, now)
                if moved is not None:
                    extra.append(moved)
            if position is not None:
                dust = await self._maybe_close(session, position, u.min_qty, now)
                if dust is not None:
                    extra.append(dust)
            if order.side == "sell" and order.excess_external:
                unit.external = await self._excess_sale(session, order, now)
            elif order.side == "sell" and order.external and order.position_id is None:
                unit.external = await self._external_sale(session, order, now)
            if unit.external is not None:
                extra.append(unit.external)
        if not changed:
            return None
        order.updated_at = now
        head_type = OrderSettled if settled else FillsApplied
        head = head_type(
            ts=now,
            cycle_id=order.cycle_id,
            order_row_id=order.id,
            fill_ids=[f.fill_id for f in new],
            position_id=order.position_id,
            status=order.status,
            filled_qty=order.filled_qty,
        )
        return (head, *extra)

    async def _buy(
        self, session: AsyncSession, order: Order, position: Position | None, fill: Fill
    ) -> Position:
        if position is None:
            adjusted = await _entry_adjusted(session, order.cycle_id)
            position = Position(
                cycle_id=order.cycle_id,
                symbol=order.symbol or fill.symbol,
                side="long",
                qty=_D0,
                entry_price=fill.price,
                stop=order.stop_price,
                tp=order.tp_price,
                opened_at=fill.ts,
                fees=_D0,
                initial_risk=_opt_dec(adjusted.get("initial_risk")),
                atr_at_entry=_opt_dec(adjusted.get("atr")),
                dust_consumed=_D0,
            )
            session.add(position)
            await session.flush()  # assigns position.id
            order.position_id = position.id
        position.qty = _norm(position.qty + _net_qty(fill))
        position.fees = _norm(
            (position.fees or _D0) + _fee_usd(fill.fee, fill.fee_asset, fill.price)
        )
        return position

    async def _sell(
        self,
        session: AsyncSession,
        order: Order,
        position: Position | None,
        fill: Fill,
        fill_row: FillRow,
        now: datetime,
    ) -> Position | None:
        """Reduce the symbol's open position. A sell beyond the open qty is carried dust of
        earlier trades, resolved only once every earlier order on the symbol is settled
        (spec 4.3): until then the whole update is deferred, never an anomaly. A marked
        external sell with no open position books no position (None, spec 4.4.1).

        Beyond the open qty and the carried dust, an unmarked row is an `oversell`; a row
        marked `excess_external` (spec 2026-11-10 5.4) consumes the dust, empties the
        position and persists the rest of the fill in `fill_row.external_qty`. The position
        is charged only the position's share of the fill's fee."""
        if position is None:
            # Never "the oldest open one": check() keeps one open journal trade per symbol,
            # so more than one is a contradiction, not a choice (PR #25 review).
            candidates = list(
                await session.scalars(
                    select(Position).where(
                        Position.symbol == order.symbol, Position.closed_at.is_(None)
                    )
                )
            )
            if len(candidates) > 1:
                raise _Anomaly(
                    "ambiguous_position", f"{len(candidates)} open {order.symbol} positions"
                )
            position = candidates[0] if candidates else None
            if position is None:
                if await _earlier_unsettled(session, order):
                    raise _Deferred("no open position yet; an earlier order is unsettled")
                if order.external:
                    return None  # decision 13: booked as an external sale at settlement
                raise _Anomaly(
                    "sell_without_position", f"sell fill {fill.fill_id}: no open {order.symbol}"
                )
            order.position_id = position.id
        external = _D0
        excess = fill.qty - position.qty
        if excess <= 0:
            position.qty = _norm(position.qty - fill.qty)
        else:
            if await _earlier_unsettled(session, order):
                raise _Deferred(
                    f"sells {excess} beyond the open qty; an earlier order is unsettled"
                )
            dust = await session.get(SymbolDust, position.symbol)
            available = _D0 if dust is None else dust.qty
            if order.excess_external:
                consumed = min(excess, available)
                external = excess - consumed
            elif dust is None or excess > available:
                raise _Anomaly(
                    "oversell", f"sells {excess} beyond the open qty, carried dust {available}"
                )
            else:
                consumed = excess
            if dust is not None and consumed > 0:
                dust.qty = _norm(available - consumed)
                dust.updated_at = now
            position.dust_consumed = _norm(position.dust_consumed + consumed)
            position.qty = _D0
            fill_row.external_qty = _norm(external)
        fee_usd = _fee_usd(fill.fee, fill.fee_asset, fill.price)
        position.fees = _norm((position.fees or _D0) + _split(fee_usd, fill.qty, external)[0])
        return position

    async def _excess_sale(
        self, session: AsyncSession, order: Order, now: datetime
    ) -> ExternalHoldingSold | None:
        """The external part of a settled `excess_external` row (spec 2026-11-10 5.4), from
        the split `_sell` persisted per fill (never recomputed). Its dust went to the
        position, so `dust_consumed` is 0 and `SymbolDust` is not touched again (never
        `_external_sale`). None when no fill had an external part."""
        await session.flush()
        rows = list(await session.scalars(select(FillRow).where(FillRow.order_id == order.id)))
        external = sum((r.external_qty for r in rows), _D0)
        if external == 0:
            return None
        proceeds = sum((_split(r.qty * r.price, r.qty, r.external_qty)[1] for r in rows), _D0)
        fees = sum(
            (_split(_fee_usd(r.fee, r.fee_asset, r.price), r.qty, r.external_qty)[1] for r in rows),
            _D0,
        )
        return ExternalHoldingSold(
            ts=now,
            cycle_id=order.cycle_id,
            order_row_id=order.id,
            symbol=order.symbol or "",
            qty=_norm(external),
            dust_consumed=_D0,
            external_qty=_norm(external),
            proceeds_usd=_norm(proceeds),
            fees_usd=_norm(fees),
        )

    async def _external_sale(
        self, session: AsyncSession, order: Order, now: datetime
    ) -> ExternalHoldingSold | None:
        """Allocate a settled external sale once, from its booked fills (never from memory,
        so partial fills and restarts can't consume the dust twice). A zero-fill order
        settles silently."""
        await session.flush()
        rows = list(await session.scalars(select(FillRow).where(FillRow.order_id == order.id)))
        sold = sum((r.qty for r in rows), _D0)
        if sold == 0:
            return None
        symbol = order.symbol or ""
        dust = await session.get(SymbolDust, symbol)
        balance = _D0 if dust is None else dust.qty
        consumed = min(sold, balance)
        if dust is not None and consumed > 0:
            dust.qty = _norm(balance - consumed)
            dust.updated_at = now
        if balance > sold:
            log.warning("ledger.dust_residual", symbol=symbol, residual=str(balance - sold))
        return ExternalHoldingSold(
            ts=now,
            cycle_id=order.cycle_id,
            order_row_id=order.id,
            symbol=symbol,
            qty=_norm(sold),
            dust_consumed=_norm(consumed),
            external_qty=_norm(sold - consumed),
            proceeds_usd=_norm(sum((r.qty * r.price for r in rows), _D0)),
            fees_usd=_norm(sum((r.fee for r in rows), _D0)),
        )

    async def _reprice(self, session: AsyncSession, order: Order, position: Position) -> None:
        """`entry_price` = gross VWAP of the entry's buy fills (Alpaca's convention)."""
        await session.flush()
        rows = list(await session.scalars(select(FillRow).where(FillRow.order_id == order.id)))
        qty = sum((r.qty for r in rows), _D0)
        vwap = sum((r.qty * r.price for r in rows), _D0) / qty
        position.entry_price = _norm(vwap.quantize(Decimal("1e-12")))

    async def _complete_cycle(
        self, session: AsyncSession, order: Order, applied: Decimal, now: datetime
    ) -> CycleTransitioned | None:
        """D15 from the order's final outcome; a terminal cycle is never reopened."""
        cycle_id = order.cycle_id
        if cycle_id is None:
            return None
        state = await repo.cycle_state(session, cycle_id)
        if state is None or state not in (CycleState.RISK_CHECK, CycleState.SUBMITTED):
            return None
        from_ = CycleState(state)
        reason: str | None
        if applied > 0:
            to, reason = CycleState.FILLED, None
            details: dict[str, object] = {
                "filled_qty": str(_norm(applied)),
                "ordered_qty": str(_norm(order.qty)),
                "partial": applied < order.qty,
            }
        elif order.broker_order_id is None:
            # Never reached the broker: the runner's own edge and wording, so the outcome
            # does not depend on who writes first (plan clarification 5).
            to, reason = CycleState.REJECTED, f"order {order.status}"
            details = {"reason": reason}
        else:
            to, reason = CycleState.EXPIRED, f"order {order.status} with no fill"
            details = {"reason": reason}
        check_edge(from_, to, reconciliation=True)
        await repo.transition_cycle(
            session, cycle_id, from_state=from_, to_state=to, now=now, reason=reason
        )
        return CycleTransitioned(
            ts=now, cycle_id=cycle_id, from_state=from_, to_state=to, details=details
        )

    async def _maybe_close(
        self, session: AsyncSession, position: Position, min_qty: Decimal | None, now: datetime
    ) -> DustLeft | None:
        """Close once every order of the position is settled and less than `min_qty` is
        left (spec 4.3/4.4; stricter than "entry settled", see the plan's clarification 1).
        Realized P&L is cash in minus cash out, so dust is written off (D17); a fill's
        external part (`excess_external`, spec 2026-11-10 5.4) is not the trade's."""
        if position.closed_at is not None or min_qty is None or position.qty >= min_qty:
            return None
        await session.flush()
        linked = list(
            await session.scalars(
                select(Order).where(Order.position_id == position.id).order_by(Order.id)
            )
        )
        if any(o.settled_at is None for o in linked):
            return None  # a late fill can still arrive: stay open (the Position Manager exits it)
        rows = list(
            await session.scalars(
                select(FillRow).where(FillRow.order_id.in_([o.id for o in linked]))
            )
        )
        exits = [o for o in linked if o.side == "sell"]
        position.closed_at = max(r.ts for r in rows)
        position.exit_reason = (exits[-1].exit_reason or "exit") if exits else "dust"
        position.realized_pnl = _norm(
            sum((_split(_cash_effect(r), r.qty, r.external_qty)[0] for r in rows), _D0)
        )
        position.dust_qty = position.qty
        log.info(
            "ledger.position_closed",
            position_id=position.id,
            exit_reason=position.exit_reason,
            realized_pnl=str(position.realized_pnl),
        )
        if position.qty == 0:
            return None
        dust = await session.get(SymbolDust, position.symbol)
        if dust is None:
            session.add(SymbolDust(symbol=position.symbol, qty=position.qty, updated_at=now))
        else:
            dust.qty = _norm(dust.qty + position.qty)
            dust.updated_at = now
        return DustLeft(
            ts=now,
            cycle_id=position.cycle_id,
            symbol=position.symbol,
            position_id=position.id,
            dust_qty=position.qty,
            min_qty=min_qty,
        )

    async def _anomaly(self, order_row_id: int, cycle_id: int | None, error: _Anomaly) -> None:
        """Pause entries for an operator in a unit of its own: the update rolled back.

        The row stays unsettled, so the sync loop books it again every pass. A repeat (the
        row's latest `LedgerAnomaly` has the same kind and detail) while entries are still
        paused writes nothing and logs `ledger.anomaly_persists` at debug level. A repeat
        after an operator resumed re-pauses: it writes a new `LedgerAnomaly` (same kind,
        detail and row) with the pause in one unit and logs `ledger.anomaly_repaused`
        (warning). The repeat check runs inside the bus unit, under the bus lock, so two
        passes cannot both write a re-pause for one resume."""
        outcome: list[Literal["new", "repaused", "persists"]] = []

        async def build(session: AsyncSession) -> DomainEvent | None:
            last = (await repo.last_anomalies(session, [order_row_id])).get(order_row_id)
            repeat = last is not None and (last["kind"], last["detail"]) == (
                error.kind,
                error.detail,
            )
            if repeat and await repo.get_control(session, PAUSED_KEY) is True:
                outcome.append("persists")
                return None
            outcome.append("repaused" if repeat else "new")
            await session.merge(ControlState(key=PAUSED_KEY, value=True))
            return LedgerAnomaly(
                ts=self._clock(),
                cycle_id=cycle_id,
                kind=error.kind,
                order_row_id=order_row_id,
                detail=error.detail,
            )

        fields = {"kind": error.kind, "order_row": order_row_id, "detail": error.detail}
        try:
            await self._bus.publish_tx(build, verify_unit)
        except Exception as e:
            # Not logged as an anomaly or a re-pause: the commit may not have landed, and a
            # warning must not claim a pause that did not. The sync loop books the row again.
            log.error("ledger.anomaly_unrecorded", error=type(e).__name__, **fields)
            raise
        if outcome == ["persists"]:
            log.debug("ledger.anomaly_persists", kind=error.kind, order_row=order_row_id)
        elif outcome == ["repaused"]:
            log.warning("ledger.anomaly_repaused", **fields)
        elif outcome == ["new"]:
            log.error("ledger.anomaly", **fields)

    # --- cash (D17) ----------------------------------------------------------------------

    async def cash_guard(self) -> CashGuard:
        """Read BEFORE the account read and pass it to `check_cash`."""
        async with self._sessions() as session:
            return await _guard(session)

    async def check_cash(self, account: Account, guard: CashGuard) -> CashResult:
        """Compare broker cash with the cash effects of fills booked since the baseline
        (spec 4.6). Compares only if the generation is unchanged from `guard` to the
        re-read inside this unit and no order was unsettled at either read: an order that
        came and went in between still changed `max(orders.id)`. Outside the tolerance:
        manual pause, `CashDiscrepancy`, re-baseline (alert once, nothing absorbed)."""
        now = self._clock()
        unit = _Unit()

        async def build(session: AsyncSession) -> DomainEvent | None:
            again = await _guard(session)
            if guard.unsettled or again.unsettled or again.generation != guard.generation:
                return None
            baseline = await repo.get_control(session, CASH_CHECK_KEY)
            await session.merge(
                ControlState(
                    key=CASH_CHECK_KEY,
                    value={
                        "cash": str(_norm(account.cash)),
                        "fill_watermark": again.generation[1],
                        "ts": now.isoformat(),
                    },
                )
            )
            if baseline is None:
                unit.result = "baseline"
                return CashChecked(
                    ts=now, result="baseline", expected=None, actual=account.cash, n_fills=0
                )
            rows = list(
                await session.scalars(
                    select(FillRow).where(FillRow.id > int(baseline["fill_watermark"]))
                )
            )
            expected = Decimal(baseline["cash"]) + sum((_cash_effect(r) for r in rows), _D0)
            diff = account.cash - expected
            if abs(diff) <= max(CENT, CENT * len(rows)):
                unit.result = "ok"
                return CashChecked(
                    ts=now, result="ok", expected=expected, actual=account.cash, n_fills=len(rows)
                )
            await session.merge(ControlState(key=PAUSED_KEY, value=True))
            unit.result = "discrepancy"
            log.error(
                "ledger.cash_discrepancy",
                expected=str(expected),
                actual=str(account.cash),
                diff=str(diff),
                n_fills=len(rows),
            )
            return CashDiscrepancy(
                ts=now,
                expected=expected,
                actual=account.cash,
                diff=diff,
                n_fills=len(rows),
                baseline_ts=str(baseline["ts"]),
            )

        await self._bus.publish_tx(build, verify_unit)
        return unit.result
