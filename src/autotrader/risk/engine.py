"""Risk Engine (SPEC §0.1, §4.5; spec 2026-10-11 §6). The only holder of the `Broker`.

`check()` gathers state under the entry lock, runs the pure `check_entry`, persists the
decision, applies a pause side effect on rejection, writes the order row, asks the
control gate again right before submitting, then submits. `exit()` is
reduce-only and ignores kill/pause. Any failure while gathering rejects (fail closed).

Two locks: entries serialise on one, exits on another, and an exit never waits for an
entry (a stalled journal call inside `check()` cannot block a stop-out). That is safe
because an entry that runs beside an exit still sees the position being exited and
rejects or over-counts it (conservative), and a late IOC fill after a kill-flatten can
happen with or without a shared lock (fills are broker-side); the ledger books it (WP 1.5b)
and the Position Manager manages it (WP 1.5c).

`check()` first syncs every unsettled order (`sync_orders`) and books it through the ledger
(`core/ledger.py`, WP 1.5b); an unreadable order rejects the entry (`state unavailable:
SyncIncomplete`). Exits never sync.

Each locked section has ONE budget of `io_timeout_s`, measured from taking its lock and
shared by its network/gate awaits only (gate.status, get_account, get_positions, each
asset_rules, get_order and order_fills, submit/close); each is wrapped in `_within`. Journal
reads and writes (every `self._sessions()` block, `_write_snapshot`) run outside any `wait_for`, so
a timeout never cancels a DB session mid-statement or mid-commit; SQLite errors on a
locked DB after `busy_timeout` (5 s) instead of hanging. A step whose budget is already
spent is not called: its timeout outcome applies, and a submit that was never attempted
is `not_submitted`. `asyncio.CancelledError` is never caught.
"""

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import Protocol, TypeVar

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import (
    Account,
    Broker,
    DustRemainder,
    Fill,
    Mode,
    OrderAck,
    OrderMethodsUnavailable,
    OrderRejected,
)
from autotrader.core.config import StrategyProfile
from autotrader.core.indicators import compute_features
from autotrader.core.kill import PAUSED_KEY, PAUSED_UNTIL_KEY, ControlStatus
from autotrader.core.tradingday import day_start, next_day_start
from autotrader.core.types import Bar, Quote
from autotrader.journal import repo
from autotrader.journal.models import Order
from autotrader.risk.checks import check_entry, check_exit
from autotrader.risk.types import (
    FINAL,
    EntryIntent,
    ExitRequest,
    HeldPosition,
    MarketSnapshot,
    OpenEntry,
    OrderUpdate,
    Pause,
    PortfolioState,
    RiskDecision,
    SyncIncomplete,
    SyncResult,
)

log = structlog.get_logger(__name__)
HOUR = timedelta(hours=1)
T = TypeVar("T")

# Final rows that never reached the broker: nothing to read, the ledger settles them.
_LOCAL_FINAL = ("not_submitted", "rejected")


class Accounting(Protocol):
    """The ledger (`core/ledger.py`, WP 1.5b): books a sync pass. Journal code only."""

    async def apply_all(self, result: SyncResult) -> None: ...


class RiskMarketView(Protocol):
    def history(self, symbol: str, timeframe: str) -> list[Bar]: ...
    def quote(self, symbol: str) -> Quote | None: ...
    def last_price(self, symbol: str) -> float | None: ...
    def is_stale(self, symbol: str) -> bool: ...


class Gate(Protocol):
    async def status(self, now: datetime | None = None) -> ControlStatus: ...


@dataclass(frozen=True)
class OrderResult:
    decision: RiskDecision
    order_row_id: int | None = None
    status: str | None = None
    broker_order_id: str | None = None
    dust: bool = False


def _dec(value: float | None) -> Decimal | None:
    return None if value is None or not math.isfinite(value) else Decimal(str(value))


class RiskEngine:
    def __init__(
        self,
        broker: Broker,
        *,
        profile: StrategyProfile,
        strategy_id: int,
        mode: Mode,
        gate: Gate,
        market: RiskMarketView,
        sessions: async_sessionmaker[AsyncSession],
        ledger: Accounting,
        trading_day_tz: str = "UTC",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        io_timeout_s: float = 10.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._broker = broker
        self._profile = profile
        self._strategy_id = strategy_id
        self._mode = mode
        self._gate = gate
        self._market = market
        self._sessions = sessions
        self._ledger = ledger
        self._tz = trading_day_tz
        self._clock = clock
        self._io_timeout = io_timeout_s
        self._monotonic = monotonic  # the budget clock (injectable for tests)
        # Monotonic time of the last committed equity snapshot (None = none yet): drives
        # run core's `risk.snapshot_overdue`, separately from the snapshot loop's liveness.
        self.last_snapshot_at: float | None = None
        self._entry_lock = asyncio.Lock()  # check(): two entries never pass check 5 together
        self._exit_lock = asyncio.Lock()  # exit(): never waits for an entry

    # --- entries ---------------------------------------------------------------------

    def can_place_orders(self) -> bool:
        return self._broker.can_place_orders()

    async def check(self, intent: EntryIntent) -> OrderResult:
        async with self._entry_lock:
            deadline = self._monotonic() + self._io_timeout
            now = self._clock()
            try:
                control, portfolio, market = await self._gather_entry(intent.symbol, now, deadline)
            except Exception as e:  # fail closed: unknown state must not allow an entry
                log.exception("risk.state_unavailable", symbol=intent.symbol)
                decision = RiskDecision(
                    approved=False, reasons=(f"state unavailable: {type(e).__name__}",)
                )
                await self._persist(intent.cycle_id, "entry", decision, now)
                return OrderResult(decision)
            decision = check_entry(
                intent,
                portfolio,
                market,
                profile=self._profile,
                control=control,
                mode_matches=self._mode == self._broker.mode(),
            )
            await self._persist(intent.cycle_id, "entry", decision, now)
            if not decision.approved or decision.order is None:
                await self._apply_pause(decision.pause, now)
                log.info("risk.entry_rejected", symbol=intent.symbol, reason=decision.reasons[0])
                return OrderResult(decision)
            order = decision.order
            async with self._sessions() as session:
                row = await repo.insert_order(
                    session,
                    cycle_id=intent.cycle_id,
                    client_order_id=order.client_order_id,
                    symbol=intent.symbol,
                    mode=self._mode,
                    type_=order.type,
                    side=order.side,
                    qty=order.qty,
                    limit_price=order.limit_price,
                    stop_price=intent.stop,
                    tp_price=intent.take_profit,
                    status="pending",
                    now=now,
                )
            try:
                final = await self._within(deadline, lambda: self._gate.status(self._clock()))
                allowed, reasons = final.entries_allowed, list(final.reasons)
            except Exception as e:  # an unanswered gate is not an allowed gate
                allowed, reasons = False, [f"gate unavailable: {type(e).__name__}"]
            if not allowed:
                await self._set_status(row.id, "not_submitted")
                log.warning("risk.entry_not_submitted", reasons=reasons)
                return OrderResult(decision, row.id, "not_submitted")
            return await self._submit(
                row.id, decision, deadline, lambda: self._broker.submit_order(order)
            )

    async def _gather_entry(
        self, symbol: str, now: datetime, deadline: float
    ) -> tuple[ControlStatus, PortfolioState, MarketSnapshot]:
        """Only the network/gate awaits are budgeted; a timeout raises out of here and
        `check()` turns it into the persisted `state unavailable` rejection."""
        # Book our own orders BEFORE reading positions: a fill the sync observed is then in
        # the journal and in the positions read below (never neither position nor entry).
        synced = await self._sync(deadline)
        await self._ledger.apply_all(synced)
        if not synced.complete:  # PLAN 3.2 item 4: an unreadable order fails entries closed
            raise SyncIncomplete(f"order rows not synced: {list(synced.failed)}")
        # After the booking: a pause this pass set (ledger anomaly) already rejects here.
        control = await self._within(deadline, lambda: self._gate.status(now))
        async with self._sessions() as session:
            open_rows = await repo.open_entry_orders(session)
            # One open journal trade per symbol (PR #25 review): a trade stays open (e.g.
            # an exit whose status lags) even when the broker holds only dust, which
            # check 5 ignores.
            journal_open = await repo.open_position_symbols(session)
        broker_filled = {u.order_row_id: u.filled_qty for u in synced.updates}
        # The sync's status is at least as new as the row's: a deferred update (spec 4.3)
        # leaves the row behind the broker, and a final order never fills its remainder.
        status = {u.order_row_id: u.status for u in synced.updates}
        account = await self._within(deadline, self._broker.get_account)
        # Persist a new peak (or the first-ever baseline) right after the account read,
        # before anything else here can fail: otherwise a spike seen by a check that then
        # rejects or fails would be forgotten and never bound drawdown. Exposure is
        # unknown at this point (NULL).
        async with self._sessions() as session:
            stored_hwm = await repo.max_hwm(session)
        if stored_hwm is None or account.equity > stored_hwm:
            await self._write_snapshot(now, account.equity, account.cash, None)
        held = await self._held_positions(deadline)
        rules = await self._within(deadline, lambda: self._broker.asset_rules(symbol))
        fees = self._broker.fee_schedule()
        async with self._sessions() as session:
            entries_hour = await repo.count_entry_orders(session, now - HOUR, inclusive=False)
            entries_day = await repo.count_entry_orders(
                session, day_start(now, self._tz), inclusive=True
            )
            baseline = await repo.day_start_equity(session, day_start(now, self._tz))
            # Re-read: the snapshot loop may have committed a higher peak while this check
            # awaited positions/rules; the decision uses the max of both durable reads.
            late_hwm = await repo.max_hwm(session)
            trades_n, wins = await repo.strategy_trade_stats(session, self._strategy_id)
        hwm = max(h for h in (stored_hwm, late_hwm, account.equity) if h is not None)
        portfolio = PortfolioState(
            equity=account.equity,
            cash=account.cash,
            day_start_equity=baseline if baseline is not None else account.equity,
            hwm=hwm,
            positions=held,
            open_entries=tuple(
                # symbol is always set by insert_order; "" only guards legacy NULL rows and
                # still counts toward max_positions (conservative). The notional is the
                # unfilled remainder: the filled part is already a priced position. A final
                # unsettled row reserves 0 but still blocks its symbol (spec 4.7).
                OpenEntry(
                    symbol=o.symbol or "",
                    notional=Decimal(0)
                    if status.get(o.id, o.status) in FINAL
                    else max(
                        o.qty - max(broker_filled.get(o.id, o.filled_qty), o.filled_qty),
                        Decimal(0),
                    )
                    * (o.limit_price or Decimal(0)),
                )
                for o in open_rows
            )
            + tuple(OpenEntry(symbol=s, notional=Decimal(0)) for s in sorted(journal_open)),
            entries_last_hour=entries_hour,
            entries_today=entries_day,
            strategy_trades_n=trades_n,
            strategy_win_rate=Decimal(wins) / Decimal(trades_n) if trades_n else None,
        )
        quote = self._market.quote(symbol)
        features = compute_features(
            self._market.history(symbol, self._profile.timeframes.signal),
            trading_day_tz=self._tz,
        )
        atr = features["atr14"][-1] if features["atr14"] else None
        market = MarketSnapshot(
            last_trade=_dec(self._market.last_price(symbol)),
            bid=_dec(quote.bid if quote else None),
            ask=_dec(quote.ask if quote else None),
            atr=_dec(atr),
            stale=self._market.is_stale(symbol),
            rules=rules,
            fees=fees,
        )
        return control, portfolio, market

    async def sync_orders(self) -> SyncResult:
        """Read-only broker view of every unsettled order for the ledger (spec 4.2). Takes
        no lock (never delays an exit) and has its own `io_timeout_s` budget."""
        return await self._sync(self._monotonic() + self._io_timeout)

    async def _sync(self, deadline: float) -> SyncResult:
        """Per-order failures (incl. `OrderNotFound` and a spent budget) are collected,
        never raised: the caller decides, and `check()` then fails closed."""
        async with self._sessions() as session:
            rows = await repo.unsettled_orders(session)
        updates: list[OrderUpdate] = []
        failed: list[int] = []
        min_qty: dict[str, Decimal] = {}
        for row in rows:
            broker_id = row.broker_order_id
            if broker_id is None:
                if row.status in _LOCAL_FINAL:
                    updates.append(
                        OrderUpdate(order_row_id=row.id, status=row.status, filled_qty=Decimal(0))
                    )
                continue  # pending / unknown without an id: the 1.5d resolver's case
            try:
                updates.append(await self._read_order(row, broker_id, min_qty, deadline))
            except Exception:
                log.exception("risk.order_sync_failed", order_row=row.id)
                failed.append(row.id)
        return SyncResult(updates=tuple(updates), failed=tuple(failed))

    async def _read_order(
        self, row: Order, broker_id: str, min_qty: dict[str, Decimal], deadline: float
    ) -> OrderUpdate:
        """Status, then (only if more filled than booked) executions: two reads, so the
        executions may be ahead of the status (the ledger handles that)."""
        status = await self._within(deadline, partial(self._broker.get_order, broker_id))
        fills: list[Fill] = []
        if status.filled_qty > row.filled_qty:
            fills = await self._within(deadline, partial(self._broker.order_fills, broker_id))
        symbol = row.symbol or ""
        if symbol not in min_qty:
            rules = await self._within(deadline, partial(self._broker.asset_rules, symbol))
            min_qty[symbol] = rules.min_qty
        return OrderUpdate(
            order_row_id=row.id,
            status=status.status,
            filled_qty=status.filled_qty,
            fills=tuple(fills),
            min_qty=min_qty[symbol],
        )

    async def _broker_positions(
        self, deadline: float, symbol: str | None = None
    ) -> tuple[HeldPosition, ...]:
        """Held positions from the broker alone, unpriced (`market_value` None). This is
        all an exit reads: no feed access, so a broken feed can't block a protective
        exit. `symbol` narrows the (per-symbol REST) rules lookups to one asset, so an
        exit depends on nothing unrelated either."""
        held = []
        for p in await self._within(deadline, self._broker.get_positions):
            if symbol is not None and p.symbol != symbol:
                continue
            rules = await self._within(deadline, partial(self._broker.asset_rules, p.symbol))
            held.append(
                HeldPosition(
                    symbol=p.symbol,
                    qty=p.qty,
                    market_value=None,
                    avg_entry_price=p.avg_entry_price,
                    min_qty=rules.min_qty,
                )
            )
        return tuple(held)

    async def _held_positions(self, deadline: float) -> tuple[HeldPosition, ...]:
        """Broker positions priced from the feed, for entries and snapshots.
        `market_value` stays None without a fresh feed price (never the cost basis):
        check 8 then rejects."""
        priced = []
        for p in await self._broker_positions(deadline):
            price = _dec(self._market.last_price(p.symbol))
            if price is not None and not self._market.is_stale(p.symbol):
                p = p.model_copy(update={"market_value": p.qty * price})
            priced.append(p)
        return tuple(priced)

    async def _within(self, deadline: float, call: Callable[[], Awaitable[T]]) -> T:
        """Run `call` in what is left of the section's budget; a spent budget raises
        TimeoutError without calling it."""
        left = deadline - self._monotonic()
        if left <= 0:
            raise TimeoutError
        return await asyncio.wait_for(call(), left)

    async def _submit(
        self,
        row_id: int,
        decision: RiskDecision,
        deadline: float,
        call: Callable[[], Awaitable[OrderAck]],
    ) -> OrderResult:
        if deadline - self._monotonic() <= 0:  # never attempted: nothing reached the broker
            log.warning("risk.order_not_submitted", order_row=row_id, reason="budget spent")
            await self._set_status(row_id, "not_submitted")
            return OrderResult(decision, row_id, "not_submitted")
        try:
            ack = await self._within(deadline, call)
        except DustRemainder as e:  # close_position only: report dust as a type (1.5c)
            log.warning("risk.exit_dust", order_row=row_id, qty=str(e.qty), min_qty=str(e.min_qty))
            await self._set_status(row_id, "rejected")
            return OrderResult(decision, row_id, "rejected", dust=True)
        except OrderRejected as e:
            log.warning("risk.order_rejected", order_row=row_id, reason=str(e))
            await self._set_status(row_id, "rejected")
            return OrderResult(decision, row_id, "rejected")
        except OrderMethodsUnavailable:  # nothing left the process
            log.warning(
                "risk.order_not_submitted", order_row=row_id, reason="broker cannot place orders"
            )
            await self._set_status(row_id, "not_submitted")
            return OrderResult(decision, row_id, "not_submitted")
        except Exception:  # timeout / network: may or may not exist at the broker
            log.exception("risk.order_unknown", order_row=row_id)
            await self._set_status(row_id, "unknown")
            return OrderResult(decision, row_id, "unknown")
        async with self._sessions() as session:
            await repo.update_order(
                session, row_id, status=ack.status, now=self._clock(), broker_order_id=ack.order_id
            )
        log.info("risk.order_submitted", order_row=row_id, broker_order_id=ack.order_id)
        return OrderResult(decision, row_id, ack.status, ack.order_id)

    async def _set_status(self, row_id: int, status: str) -> None:
        async with self._sessions() as session:
            await repo.update_order(session, row_id, status=status, now=self._clock())

    async def _persist(
        self, cycle_id: int | None, kind: str, decision: RiskDecision, now: datetime
    ) -> None:
        async with self._sessions() as session:
            await repo.record_risk_decision(
                session,
                cycle_id=cycle_id,
                kind=kind,
                approved=decision.approved,
                reasons=list(decision.reasons),
                adjusted=dict(decision.adjusted),
                computed_size=decision.qty,
                ts=now,
            )

    async def _apply_pause(self, pause: Pause | None, now: datetime) -> None:
        if pause is None:
            return
        async with self._sessions() as session:
            if pause == "until_next_day":
                until = next_day_start(now, self._tz)
                await repo.set_control(session, PAUSED_UNTIL_KEY, until.isoformat())
                log.warning("risk.paused_until", until=until.isoformat())
            else:
                await repo.set_control(session, PAUSED_KEY, True)
                log.warning("risk.paused_manual_resume")

    # --- exits -----------------------------------------------------------------------

    async def exit(self, request: ExitRequest) -> OrderResult:
        """Reduce-only full exit (G1). No gate check: kill and pause never block it.
        Carries no client_order_id because `Broker.close_position` takes none."""
        async with self._exit_lock:
            deadline = self._monotonic() + self._io_timeout
            now = self._clock()
            try:
                held = await self._broker_positions(deadline, request.symbol)
            except Exception as e:  # fail closed: unknown positions must not size an exit
                log.exception("risk.exit_state_unavailable", symbol=request.symbol)
                decision = RiskDecision(
                    approved=False, reasons=(f"state unavailable: {type(e).__name__}",)
                )
                await self._persist(request.cycle_id, "exit", decision, now)
                return OrderResult(decision)
            decision = check_exit(request, held, mode_matches=self._mode == self._broker.mode())
            await self._persist(request.cycle_id, "exit", decision, now)
            if decision.qty is None:
                log.warning("risk.exit_rejected", symbol=request.symbol, reason=decision.reasons[0])
                return OrderResult(decision)
            async with self._sessions() as session:
                row = await repo.insert_order(
                    session,
                    cycle_id=request.cycle_id,
                    client_order_id=None,
                    symbol=request.symbol,
                    mode=self._mode,
                    type_="market",
                    side="sell",
                    qty=decision.qty,
                    limit_price=None,
                    stop_price=None,
                    tp_price=None,
                    status="pending",
                    now=now,
                    exit_reason=request.reason,
                )
            return await self._submit(
                row.id, decision, deadline, lambda: self._broker.close_position(request.symbol)
            )

    # --- equity snapshots ------------------------------------------------------------

    async def snapshot_equity(self) -> Account:
        """Called every 60 s by `run core`'s supervised `risk.snapshot` loop. Its broker calls
        have their own `io_timeout_s` budget; journal writes are never cancelled.
        Lock-free on purpose: it must never delay an exit. Returns the account it read, so a
        caller can feed the ledger's cash check (1.5d) without a second broker call."""
        deadline = self._monotonic() + self._io_timeout  # its own budget; no lock shared
        now = self._clock()
        account = await self._within(deadline, self._broker.get_account)
        try:
            held = await self._held_positions(deadline)
        except Exception:  # the equity (and a peak) is still recorded, exposure unknown
            await self._write_snapshot(now, account.equity, account.cash, None)
            raise
        await self._write_snapshot(now, account.equity, account.cash, held)
        return account

    async def _write_snapshot(
        self, now: datetime, equity: Decimal, cash: Decimal, held: tuple[HeldPosition, ...] | None
    ) -> None:
        """Append one row; `held=None` (positions not read) stores exposure NULL."""
        async with self._sessions() as session:
            stored_hwm = await repo.max_hwm(session)
            baseline = await repo.day_start_equity(session, day_start(now, self._tz))
            hwm = equity if stored_hwm is None else max(stored_hwm, equity)
            await repo.insert_equity_snapshot(
                session,
                ts=now,
                equity=equity,
                cash=cash,
                # Informational only, never used for a limit: an unpriced holding is
                # estimated at cost basis here (check 8 rejects on it instead).
                exposure=None
                if held is None
                else sum(
                    (
                        p.market_value if p.market_value is not None else p.qty * p.avg_entry_price
                        for p in held
                    ),
                    Decimal(0),
                ),
                hwm=hwm,
                daily_pnl=equity - (baseline if baseline is not None else equity),
            )
            self.last_snapshot_at = self._monotonic()
