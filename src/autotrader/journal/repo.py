"""Journal access. Functions take an `AsyncSession` and commit their own unit of work,
except the cycle writes (`insert_cycle`, `transition_cycle`) and `append_event(...,
commit=False)`: those run inside `EventBus.publish_tx`, which commits the row together with
its event (WP 1.4, D15).

Phase 0 needs live here (strategies, bars, events, control state), plus the read-only
queries the Scanner (WP 1.2) runs on cycles and positions; WP 1.3 adds the risk-decision,
order and equity-snapshot functions. The other tables get their write functions in the
work package that writes them. There is deliberately no
update/delete for events and no delete anywhere (SPEC §4.8: nothing is deleted).
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from typing import cast as typing_cast

from sqlalchemy import Float, cast, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from autotrader.core.config import StrategyProfile
from autotrader.core.types import Bar
from autotrader.journal.models import (
    BarRow,
    ControlState,
    Cycle,
    EquitySnapshot,
    Event,
    Order,
    Position,
    RiskDecision,
    Strategy,
)

# SPEC §4.3: a cycle in any other state is still open.
TERMINAL_CYCLE_STATES = frozenset({"FILLED", "REJECTED", "EXPIRED", "ERROR"})


def _now() -> datetime:
    return datetime.now(UTC)


# --- strategies ----------------------------------------------------------------------


async def register_strategy(
    session: AsyncSession, profile: StrategyProfile, yaml_text: str, now: datetime | None = None
) -> Strategy:
    """Make `profile` the active strategy; one row per profile hash.

    Already active → no-op. Otherwise the previously active row is closed and this
    profile's row is (re)opened, so cycles keep pointing at the exact profile they ran.
    """
    now = now or _now()
    # populate_existing: sessions use expire_on_commit=False, so a row cached earlier in
    # this session could still look active after another session switched profiles.
    row = await session.scalar(
        select(Strategy)
        .where(Strategy.hash == profile.hash)
        .execution_options(populate_existing=True)
    )
    if row is not None and row.active_to is None:
        return row
    await session.execute(
        update(Strategy).where(Strategy.active_to.is_(None)).values(active_to=now)
    )
    if row is None:
        row = Strategy(name=profile.name, yaml=yaml_text, hash=profile.hash, active_from=now)
        session.add(row)
    else:
        row.active_from, row.active_to = now, None
    await session.commit()
    return row


# --- bars ----------------------------------------------------------------------------

# Rows per INSERT: 8 bind params each -> 8,000 params, well under SQLite's 32,766 and
# asyncpg's 32,767 per statement. A 9-day x 3-symbol backfill is ~39k rows.
_BAR_BATCH = 1000


async def upsert_bars(session: AsyncSession, bars: Sequence[Bar]) -> None:
    """Insert bars; an existing (symbol, timeframe, ts) is overwritten (backfill fixes)."""
    if not bars:
        return
    rows = [
        {
            "symbol": b.symbol,
            "timeframe": b.timeframe,
            "ts": b.ts,
            "o": b.open,
            "h": b.high,
            "l": b.low,
            "c": b.close,
            "v": b.volume,
        }
        for b in bars
    ]
    dialect = (await session.connection()).dialect.name
    insert = pg_insert if dialect == "postgresql" else sqlite_insert
    for i in range(0, len(rows), _BAR_BATCH):
        stmt = insert(BarRow).values(rows[i : i + _BAR_BATCH])
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "timeframe", "ts"],
            set_={c: stmt.excluded[c] for c in ("o", "h", "l", "c", "v")},
        )
        await session.execute(stmt)
    await session.commit()  # one transaction for the whole batch


async def latest_bar_ts(session: AsyncSession, symbol: str, timeframe: str) -> datetime | None:
    return await session.scalar(
        select(func.max(BarRow.ts)).where(BarRow.symbol == symbol, BarRow.timeframe == timeframe)
    )


async def get_bars(
    session: AsyncSession,
    symbol: str,
    timeframe: str,
    since: datetime,
    until: datetime | None = None,
) -> list[Bar]:
    """Bars with `since <= ts < until`, oldest first."""
    query = select(BarRow).where(
        BarRow.symbol == symbol, BarRow.timeframe == timeframe, BarRow.ts >= since
    )
    if until is not None:
        query = query.where(BarRow.ts < until)
    rows = await session.scalars(query.order_by(BarRow.ts))
    return [
        Bar(
            symbol=r.symbol,
            timeframe=r.timeframe,
            ts=r.ts,
            open=r.o,
            high=r.h,
            low=r.l,
            close=r.c,
            volume=r.v,
        )
        for r in rows
    ]


# --- events --------------------------------------------------------------------------


async def append_event(
    session: AsyncSession,
    type_: str,
    payload: dict[str, Any],
    *,
    cycle_id: int | None = None,
    ts: datetime | None = None,
    commit: bool = True,
) -> Event:
    """Append one event. `commit=False` only flushes: the caller's transaction decides."""
    row = Event(ts=ts or _now(), type=type_, cycle_id=cycle_id, payload=payload)
    session.add(row)
    if commit:
        await session.commit()
    else:
        await session.flush()
    return row


async def events_after(session: AsyncSession, after_id: int, limit: int = 500) -> list[Event]:
    """Events with id > `after_id`, oldest first (dashboard tail, WS replay)."""
    rows = await session.scalars(
        select(Event).where(Event.id > after_id).order_by(Event.id).limit(limit)
    )
    return list(rows)


async def events_since(session: AsyncSession, type_: str, since: datetime) -> list[Event]:
    """Events of `type_` with ts > `since`, oldest first."""
    rows = await session.scalars(
        select(Event).where(Event.type == type_, Event.ts > since).order_by(Event.id)
    )
    return list(rows)


# --- cycles / positions (read side) --------------------------------------------------


async def open_cycle_symbols(session: AsyncSession) -> list[str]:
    """Symbol of every open (non-terminal) cycle; one entry per cycle."""
    rows = await session.scalars(
        select(Cycle.symbol).where(Cycle.state.not_in(TERMINAL_CYCLE_STATES))
    )
    return list(rows)


async def open_position_symbols(session: AsyncSession) -> set[str]:
    rows = await session.scalars(select(Position.symbol).where(Position.closed_at.is_(None)))
    return set(rows)


# --- cycles (WP 1.4) -----------------------------------------------------------------
# The two writes below never commit: they run inside `EventBus.publish_tx`, which
# commits the cycle row together with its `CycleTransitioned` event (D15).


class StaleTransition(RuntimeError):
    """The guarded update matched no row: the cycle is not in the expected state."""


async def insert_cycle(
    session: AsyncSession,
    *,
    strategy_id: int,
    symbol: str,
    rule_id: str,
    signal_features: dict[str, Any],
    state: str,
    now: datetime,
) -> Cycle:
    row = Cycle(
        strategy_id=strategy_id,
        symbol=symbol,
        rule_id=rule_id,
        signal_features=signal_features,
        state=state,
        created_at=now,
    )
    session.add(row)
    await session.flush()  # assigns row.id
    return row


async def transition_cycle(
    session: AsyncSession,
    cycle_id: int,
    *,
    from_state: str,
    to_state: str,
    now: datetime,
    reason: str | None = None,
) -> None:
    """`from_state -> to_state` only if the row is still in `from_state`.

    Terminal states also set `outcome`, `finished_at` and `reject_reason`.
    """
    values: dict[str, Any] = {"state": to_state}
    if to_state in TERMINAL_CYCLE_STATES:
        values.update(outcome=to_state, finished_at=now, reject_reason=reason)
    result = typing_cast(
        CursorResult[Any],
        await session.execute(
            update(Cycle).where(Cycle.id == cycle_id, Cycle.state == from_state).values(**values)
        ),
    )
    if result.rowcount != 1:
        raise StaleTransition(f"cycle {cycle_id} is not in state {from_state}")


async def cycle_state(session: AsyncSession, cycle_id: int) -> str | None:
    return await session.scalar(select(Cycle.state).where(Cycle.id == cycle_id))


async def open_cycles(session: AsyncSession) -> list[Cycle]:
    rows = await session.scalars(
        select(Cycle)
        .where(Cycle.state.not_in(TERMINAL_CYCLE_STATES))
        .order_by(Cycle.id)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def has_order_for_cycle(session: AsyncSession, cycle_id: int) -> bool:
    n = await session.scalar(
        select(func.count()).select_from(Order).where(Order.cycle_id == cycle_id)
    )
    return bool(n)


# --- control state -------------------------------------------------------------------


async def get_control(session: AsyncSession, key: str) -> Any:
    row = await session.get(ControlState, key)
    return None if row is None else row.value


async def set_control(session: AsyncSession, key: str, value: Any) -> None:
    await session.merge(ControlState(key=key, value=value))
    await session.commit()


# --- risk decisions / orders / equity (WP 1.3) ---------------------------------------


async def record_risk_decision(
    session: AsyncSession,
    *,
    cycle_id: int | None,
    kind: str,
    approved: bool,
    reasons: list[str],
    adjusted: dict[str, str],
    computed_size: Decimal | None,
    ts: datetime,
) -> RiskDecision:
    row = RiskDecision(
        cycle_id=cycle_id,
        kind=kind,
        approved=approved,
        reasons=reasons,
        adjusted=adjusted,
        computed_size=computed_size,
        ts=ts,
    )
    session.add(row)
    await session.commit()
    return row


async def insert_order(
    session: AsyncSession,
    *,
    cycle_id: int | None,
    client_order_id: str | None,
    symbol: str,
    mode: str,
    type_: str,
    side: str,
    qty: Decimal,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    tp_price: Decimal | None,
    status: str,
    now: datetime,
    exit_reason: str | None = None,
) -> Order:
    row = Order(
        cycle_id=cycle_id,
        client_order_id=client_order_id,
        symbol=symbol,
        mode=mode,
        type=type_,
        side=side,
        qty=qty,
        limit_price=limit_price,
        stop_price=stop_price,
        tp_price=tp_price,
        status=status,
        submitted_at=now,
        updated_at=now,
        exit_reason=exit_reason,
    )
    session.add(row)
    await session.commit()
    return row


async def update_order(
    session: AsyncSession,
    order_id: int,
    *,
    status: str,
    now: datetime,
    broker_order_id: str | None = None,
) -> None:
    values: dict[str, Any] = {"status": status, "updated_at": now}
    if broker_order_id is not None:
        values["broker_order_id"] = broker_order_id
    await session.execute(update(Order).where(Order.id == order_id).values(**values))
    await session.commit()


async def open_entry_orders(session: AsyncSession) -> list[Order]:
    """Entry orders that still reserve their symbol: every buy not yet settled by the
    ledger (WP 1.5b), whatever its status. A final but unsettled row (fills not booked
    yet) keeps reserving until the ledger settles it."""
    rows = await session.scalars(
        select(Order)
        .where(Order.side == "buy", Order.settled_at.is_(None))
        .order_by(Order.id)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def unsettled_orders(session: AsyncSession) -> list[Order]:
    """Every order whose effects are not fully applied yet (WP 1.5b), in id order: the
    ledger books a symbol's orders oldest first."""
    rows = await session.scalars(
        select(Order)
        .where(Order.settled_at.is_(None))
        .order_by(Order.id)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def count_entry_orders(session: AsyncSession, since: datetime, *, inclusive: bool) -> int:
    """Entry orders that were sent (or may have been) since `since`; `not_submitted`
    never reached the broker and does not count."""
    after = Order.submitted_at >= since if inclusive else Order.submitted_at > since
    count = await session.scalar(
        select(func.count())
        .select_from(Order)
        .where(Order.side == "buy", Order.status != "not_submitted", after)
    )
    return int(count or 0)


async def insert_equity_snapshot(
    session: AsyncSession,
    *,
    ts: datetime,
    equity: Decimal,
    cash: Decimal,
    exposure: Decimal | None,
    hwm: Decimal,
    daily_pnl: Decimal,
) -> None:
    session.add(
        EquitySnapshot(
            ts=ts, equity=equity, cash=cash, exposure=exposure, hwm=hwm, daily_pnl=daily_pnl
        )
    )
    await session.commit()


async def latest_snapshot(session: AsyncSession) -> EquitySnapshot | None:
    return await session.scalar(select(EquitySnapshot).order_by(EquitySnapshot.ts.desc()).limit(1))


async def max_hwm(session: AsyncSession) -> Decimal | None:
    """Highest HWM ever stored (None without snapshots). The maximum over all rows, not the
    latest row's value, so a writer that interleaved with a peak can never lower it.

    SQLite stores `Money` as text, where a plain `max()` compares strings ("999" > "1200").
    So the float cast only narrows the candidates; the exact Decimals are compared here.
    """
    as_float = cast(EquitySnapshot.hwm, Float)
    top = await session.scalar(select(func.max(as_float)))
    if top is None:
        return None
    return max(await session.scalars(select(EquitySnapshot.hwm).distinct().where(as_float >= top)))


async def day_start_equity(session: AsyncSession, day_start: datetime) -> Decimal | None:
    """Equity of the last snapshot at or before `day_start`; if there is none (first day
    of operation), the first snapshot after it. None without any snapshot."""
    before = await session.scalar(
        select(EquitySnapshot.equity)
        .where(EquitySnapshot.ts <= day_start)
        .order_by(EquitySnapshot.ts.desc())
        .limit(1)
    )
    if before is not None:
        return before
    return await session.scalar(
        select(EquitySnapshot.equity)
        .where(EquitySnapshot.ts > day_start)
        .order_by(EquitySnapshot.ts)
        .limit(1)
    )


async def strategy_trade_stats(session: AsyncSession, strategy_id: int) -> tuple[int, int]:
    """(closed trades, wins) of a strategy; a win is realized P&L > 0 after fees."""
    closed = (
        select(Position.realized_pnl)
        .join(Cycle, Position.cycle_id == Cycle.id)
        .where(Cycle.strategy_id == strategy_id, Position.closed_at.is_not(None))
    )
    pnls = list(await session.scalars(closed))
    return len(pnls), sum(1 for p in pnls if p is not None and p > 0)
