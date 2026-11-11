"""Risk Engine (SPEC §0.1, §4.5; spec 2026-10-11 §6). The only holder of the `Broker`.

`check()` gathers state under the entry lock, runs the pure `check_entry`, persists the
decision, applies a pause side effect on rejection, writes the order row, asks the
control gate and re-checks engine health right before submitting, then submits. `exit()` is
reduce-only and ignores kill/pause; it sells the free qty through `submit_order` like an
entry does (`close_position` is never called). Every order carries a journal-namespaced client
id (`risk/resolve.py`); an entry fails closed without the journal id, an exit goes out without
a client id (logged). Any failure while gathering rejects (fail closed).

Two locks: entries serialise on one, exits on another, and an exit never waits for an
entry (a stalled journal call inside `check()` cannot block a stop-out). That is safe
because an entry that runs beside an exit still sees the position being exited and
rejects or over-counts it (conservative), and a late IOC fill after a kill-flatten can
happen with or without a shared lock (fills are broker-side); the ledger books it (WP 1.5b)
and the Position Manager manages it (WP 1.5c).

`check()` first reads every unsettled order (the read step of the sync) and books it through
the ledger (`core/ledger.py`, WP 1.5b); an unreadable order, or a symbol whose booking raised in
the ledger, rejects every entry (`state unavailable: SyncIncomplete`). Exits never sync.
After that sync, a symbol with any unsettled sell row (own or external) rejects its entries.

Order resolver (WP 1.5d-2, spec 2026-11-10 3): only `sync_orders()` (the ledger's sync loop)
resolves, after the same read step and under the same budget; `check()` only reads. Candidates
are (a) `pending`/`unknown` rows without a broker id that are not in flight and (b) rows whose
`get_order` raised `OrderNotFound` (clock `resolve_since`, persisted at the first miss and kept
until a successful read). A row is looked up by its own journal-namespaced client id: a match
links a type (a) row; a row without ownership proof or with a mismatching hit becomes an
incident at once; otherwise the row becomes an incident once `order_resolution_timeout_s` has
passed since `submitted_at` (a) or `resolve_since` (b). A lookup the budget did not reach is
"not looked up", never a zero match. Incident rows are skipped by the read and by the resolver
until an operator repairs them; `risk.order_incident_open` reminds every 60 s. The resolver
never marks a row `not_submitted` (D20.2): a zero match is no proof that an order cannot exist.

Daily-loss baseline (D19, spec 2026-11-09 3): the equity of the latest snapshot observed in
the `2 x snapshot_interval_s` before the trading-day start; without one, entries are rejected
(`daily-loss baseline unavailable`) for the whole day. No fallback to an older or later
snapshot or to current equity; `repo.day_start_equity` feeds only the informational
`daily_pnl` column. Every snapshot row is stamped with the engine clock read after
`get_account()` returned (receipt time), so a read that crosses 00:00 never counts as a
pre-boundary baseline. `last_snapshot_at` follows the snapshot loop (`snapshot_equity()`)
only, never the peak snapshot `check()` writes.

Health faults (spec 2026-11-09 4, 5.2, 6): `health_faults()` is read at the start of
`_gather_entry` and reaches `check_entry(faults=...)`, which rejects with the first one right
after check 1 and sets no pause. In order: `equity snapshot overdue` (no loop snapshot yet in
this process, or one older than `2 x snapshot_interval_s`) and `cash reconciliation failed`
(`cash_fault`, initially `not yet reconciled`; cleared by `cash_reconciled()`; a sticky fault,
`CommitUncertain`, holds until restart and is never downgraded). After the pre-submit gate
re-check, `check()` reads `health_faults()` again, plus `trading day changed` when the engine
clock's trading day differs from the gather's (spec 2026-11-09 6.1): any of them leaves the
order row `not_submitted` (`risk.entry_not_submitted`) and nothing is sent. Exits ignore all.

Each locked section has ONE budget of `io_timeout_s`, measured from taking its lock and
shared by its network/gate awaits only (gate.status, get_account, get_positions, each
asset_rules, get_order and order_fills, submit_order); each is wrapped in `_within`. Journal
reads and writes (every `self._sessions()` block, `_write_snapshot`) run outside any `wait_for`, so
a timeout never cancels a DB session mid-statement or mid-commit; SQLite errors on a
locked DB after `busy_timeout` (5 s) instead of hanging. A step whose budget is already
spent is not called: its timeout outcome applies, and a submit that was never attempted
is `not_submitted`. `asyncio.CancelledError` is never caught.

`cancel_open_entries()` cancels resting entry (buy) orders under the exit lock with its own
budget, and `sellable_positions()` reads the non-dust broker holdings lock-free with its own.
"""

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import Literal, Protocol, TypeVar

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import (
    Account,
    Broker,
    BrokerOrder,
    BrokerOrderView,
    DustRemainder,
    Fill,
    Mode,
    OrderAck,
    OrderMethodsUnavailable,
    OrderNotFound,
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
from autotrader.risk.resolve import IncidentReason, exit_client_id, mismatches, owns
from autotrader.risk.types import (
    FINAL,
    CancelResult,
    EntryIntent,
    ExitRequest,
    HeldPosition,
    MarketSnapshot,
    OpenEntry,
    OrderResolution,
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
# How often `risk.order_incident_open` repeats while any incident row is unsettled.
_INCIDENT_REMINDER_S = 60.0


@dataclass
class _Read:
    """The read step of one sync pass: the ledger's `SyncResult` parts plus the resolver's
    candidates, in row order (`b` = `get_order` raised `OrderNotFound`)."""

    updates: list[OrderUpdate] = field(default_factory=list)
    failed: list[int] = field(default_factory=list)
    resolutions: list[OrderResolution] = field(default_factory=list)
    candidates: list[tuple[Order, Literal["a", "b"]]] = field(default_factory=list)
    incidents: dict[int, str] = field(default_factory=dict)  # unsettled rows already marked

    def result(self) -> SyncResult:
        return SyncResult(
            updates=tuple(self.updates),
            failed=tuple(self.failed),
            resolutions=tuple(self.resolutions),
        )


class Accounting(Protocol):
    """The ledger (`core/ledger.py`, WP 1.5b): books a sync pass. Journal code only."""

    async def apply_all(self, result: SyncResult) -> frozenset[str]: ...


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
    min_qty: Decimal | None = None  # the asset's min_qty when the holding is dust
    no_position: bool = False  # the broker holds nothing in the symbol


def _dec(value: float | None) -> Decimal | None:
    return None if value is None or not math.isfinite(value) else Decimal(str(value))


async def read_order_by_client_id(
    broker: Broker, client_order_id: str, *, timeout_s: float
) -> BrokerOrderView | None:
    """The `repair link` broker read (spec 5.2): the CLI hands its broker here, so only this
    module holds a `Broker` (SPEC 0.1) and `core/repair.py` gets a lookup callable. Raises
    `TimeoutError` past `timeout_s`; broker errors propagate."""
    return await asyncio.wait_for(broker.get_order_by_client_id(client_order_id), timeout_s)


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
        snapshot_interval_s: float = 60.0,
        order_visibility_allowance_s: float = 60.0,
        order_resolution_timeout_s: float = 80.0,
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
        # The snapshot loop's period; the trusted baseline window is twice it (D19).
        self._snapshot_interval_s = snapshot_interval_s
        # Monotonic time of the last equity snapshot the snapshot LOOP committed (None = none
        # yet); the peak snapshot `check()` writes never sets it (spec 2026-11-09 4). Drives
        # run core's `risk.snapshot_overdue`, separately from the snapshot loop's liveness.
        self.last_snapshot_at: float | None = None
        # Cash-reconciliation fault (spec 2026-11-09 5.2): in memory, per process. Set until
        # the cash step has reconciled once; sticky (CommitUncertain) holds until restart.
        self.cash_fault: str | None = "not yet reconciled"
        self.cash_fault_sticky = False
        self._entry_lock = asyncio.Lock()  # check(): two entries never pass check 5 together
        self._exit_lock = asyncio.Lock()  # exit(): never waits for an entry
        self._cached_journal_id: str | None = None
        # Row ids of orders `check()`/`exit()` is handling right now: added once the row is
        # inserted, dropped when the call returns. The order resolver (spec 2026-11-10) skips
        # these rows; it never links a row whose submit is still in progress.
        self._in_flight: set[int] = set()
        # Order resolver bounds (spec 2026-11-10 3.5): a miss before submit + io timeout +
        # allowance is expected (debug); an unlinked row becomes an incident at the timeout.
        self._visibility_allowance = timedelta(seconds=order_visibility_allowance_s)
        self._resolution_timeout = timedelta(seconds=order_resolution_timeout_s)
        # Last lookup outcome per candidate row, for the incident reason when a later pass
        # cannot look it up (spent budget). Per process: after a restart the reason defaults.
        self._last_outcome: dict[int, IncidentReason] = {}
        self._incident_logged_at: float | None = None  # monotonic time of the last reminder

    async def _journal_id(self) -> str | None:
        """The journal's id (client ids embed it), read once and cached when found."""
        if self._cached_journal_id is None:
            async with self._sessions() as session:
                self._cached_journal_id = await repo.journal_id(session)
        return self._cached_journal_id

    # --- health: entry faults (spec 2026-11-09 4, 5.2, 6) ------------------------------

    def set_cash_fault(self, reason: str, *, sticky: bool = False) -> None:
        """Fail entries closed until `cash_reconciled()`. Stickiness only ever goes from
        false to true: a later plain fault replaces the reason of a non-sticky one but never
        downgrades a sticky one. Logged on transitions only (clear -> set, or becoming sticky)."""
        was_set, was_sticky = self.cash_fault is not None, self.cash_fault_sticky
        if not was_sticky:
            self.cash_fault = reason
        self.cash_fault_sticky = was_sticky or sticky
        if not was_set or (self.cash_fault_sticky and not was_sticky):
            log.warning("risk.cash_fault_set", reason=self.cash_fault, sticky=sticky)

    def cash_reconciled(self) -> None:
        """Clear the cash fault; a no-op while it is sticky."""
        if self.cash_fault_sticky or self.cash_fault is None:
            return
        self.cash_fault = None
        log.info("risk.cash_fault_cleared")

    def health_faults(self) -> tuple[str, ...]:
        """Reasons that reject every entry, read with the engine clock right now: no loop
        snapshot yet or one older than `2 x snapshot_interval_s`, then the cash fault."""
        faults: list[str] = []
        last = self.last_snapshot_at
        if last is None or self._monotonic() - last > 2 * self._snapshot_interval_s:
            faults.append("equity snapshot overdue")
        if self.cash_fault is not None:
            faults.append("cash reconciliation failed")
        return tuple(faults)

    # --- entries ---------------------------------------------------------------------

    def can_place_orders(self) -> bool:
        return self._broker.can_place_orders()

    async def check(self, intent: EntryIntent) -> OrderResult:
        async with self._entry_lock:
            deadline = self._monotonic() + self._io_timeout
            now = self._clock()
            try:
                control, portfolio, market, faults, jid = await self._gather_entry(
                    intent.symbol, now, deadline
                )
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
                faults=faults,
                journal_id=jid,
            )
            await self._persist(intent.cycle_id, "entry", decision, now)
            if not decision.approved or decision.order is None:
                await self._apply_pause(decision.pause, now)
                log.info("risk.entry_rejected", symbol=intent.symbol, reason=decision.reasons[0])
                return OrderResult(decision)
            order = decision.order
            claimed: list[int] = []  # the row id this call put into `_in_flight`
            try:
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
                    # Inside the block, before the session closes (an await): see `_in_flight`.
                    # The `finally` below covers that close too: a cancel there must not leak it.
                    self._in_flight.add(row.id)
                    claimed.append(row.id)
                try:
                    final = await self._within(deadline, lambda: self._gate.status(self._clock()))
                    allowed, reasons = final.entries_allowed, list(final.reasons)
                except Exception as e:  # an unanswered gate is not an allowed gate
                    allowed, reasons = False, [f"gate unavailable: {type(e).__name__}"]
                if not allowed:
                    await self._set_status(row.id, "not_submitted")
                    log.warning("risk.entry_not_submitted", reasons=reasons)
                    return OrderResult(decision, row.id, "not_submitted")
                # Health may have turned bad since the gather (spec 2026-11-09 6.1):
                # synchronous state and clock reads only, so no await separates this from
                # the submit.
                late = (
                    *self.health_faults(),
                    *(
                        ("trading day changed",)
                        if day_start(self._clock(), self._tz) != day_start(now, self._tz)
                        else ()
                    ),
                )
                if late:
                    await self._set_status(row.id, "not_submitted")
                    log.warning("risk.entry_not_submitted", reasons=list(late))
                    return OrderResult(decision, row.id, "not_submitted")
                return await self._submit(
                    row.id, decision, deadline, lambda: self._broker.submit_order(order)
                )
            finally:
                self._in_flight.difference_update(claimed)

    async def _gather_entry(
        self, symbol: str, now: datetime, deadline: float
    ) -> tuple[ControlStatus, PortfolioState, MarketSnapshot, tuple[str, ...], str]:
        """Only the network/gate awaits are budgeted; a timeout raises out of here and
        `check()` turns it into the persisted `state unavailable` rejection. The health
        faults are read first, before the order sync; the gather runs fully either way."""
        faults = self.health_faults()
        jid = await self._journal_id()
        if jid is None:  # entries fail closed without the id their client ids embed
            raise RuntimeError("journal has no journal_id")
        # Book our own orders BEFORE reading positions: a fill the sync observed is then in
        # the journal and in the positions read below (never neither position nor entry).
        synced = await self._sync(deadline)
        failed = await self._ledger.apply_all(synced)
        if not synced.complete or failed:  # PLAN 3.2 item 4 / spec 4.1: fail entries closed
            raise SyncIncomplete(
                f"order rows not synced: {list(synced.failed)}; "
                f"symbols the ledger could not book: {sorted(failed)}"
            )
        # After the booking: a pause this pass set (ledger anomaly) already rejects here.
        control = await self._within(deadline, lambda: self._gate.status(now))
        async with self._sessions() as session:
            open_rows = await repo.open_entry_orders(session)
            # A sell (own or external) whose effects are not booked yet blocks its symbol.
            unsettled_sells = frozenset(await repo.unsettled_sell_symbols(session))
            # One open journal trade per symbol (PR #25 review): a trade stays open (e.g.
            # an exit whose status lags) even when the broker holds only dust, which
            # check 5 ignores.
            journal_open = await repo.open_position_symbols(session)
        broker_filled = {u.order_row_id: u.filled_qty for u in synced.updates}
        # The sync's status is at least as new as the row's: a deferred update (spec 4.3)
        # leaves the row behind the broker, and a final order never fills its remainder.
        status = {u.order_row_id: u.status for u in synced.updates}
        account = await self._within(deadline, self._broker.get_account)
        observed = self._clock()  # receipt time: the row's stamp (spec 2026-11-09 3.1)
        # Persist a new peak (or the first-ever snapshot) right after the account read,
        # before anything else here can fail: otherwise a spike seen by a check that then
        # rejects or fails would be forgotten and never bound drawdown. Exposure is
        # unknown at this point (NULL). Stamped at receipt, not at the gather's `now`, so
        # a read that crosses 00:00 never passes as a pre-boundary baseline. It does not
        # refresh `last_snapshot_at` (only the snapshot loop does).
        async with self._sessions() as session:
            stored_hwm = await repo.max_hwm(session)
        if stored_hwm is None or account.equity > stored_hwm:
            await self._write_snapshot(observed, account.equity, account.cash, None)
        held = await self._held_positions(deadline)
        rules = await self._within(deadline, lambda: self._broker.asset_rules(symbol))
        fees = self._broker.fee_schedule()
        async with self._sessions() as session:
            entries_hour = await repo.count_entry_orders(session, now - HOUR, inclusive=False)
            entries_day = await repo.count_entry_orders(
                session, day_start(now, self._tz), inclusive=True
            )
            # Only a snapshot observed in the 2 x interval before the day start is a baseline
            # (D19); None rejects in check 3. Never `repo.day_start_equity` (informational).
            baseline = await repo.trusted_day_start_equity(
                session,
                day_start(now, self._tz),
                timedelta(seconds=2 * self._snapshot_interval_s),
            )
            # Re-read: the snapshot loop may have committed a higher peak while this check
            # awaited positions/rules; the decision uses the max of both durable reads.
            late_hwm = await repo.max_hwm(session)
            trades_n, wins = await repo.strategy_trade_stats(session, self._strategy_id)
        hwm = max(h for h in (stored_hwm, late_hwm, account.equity) if h is not None)
        portfolio = PortfolioState(
            equity=account.equity,
            cash=account.cash,
            day_start_equity=baseline,
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
            unsettled_sells=unsettled_sells,
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
        return control, portfolio, market, faults, jid

    async def sync_orders(self) -> SyncResult:
        """Broker view of every unsettled order for the ledger (spec 4.2), then the order
        resolver's decisions (spec 2026-11-10 3), under ONE `io_timeout_s` budget. Takes no
        lock (never delays an exit)."""
        deadline = self._monotonic() + self._io_timeout
        return await self._resolve(await self._read(deadline), deadline)

    async def _sync(self, deadline: float) -> SyncResult:
        """The read step alone (`check()`): it never spends an entry's budget on lookups."""
        return (await self._read(deadline)).result()

    async def _read(self, deadline: float) -> _Read:
        """Per-order failures (incl. `OrderNotFound` and a spent budget) are collected,
        never raised: the caller decides, and `check()` then fails closed. Incident rows are
        not read (the operator has them); a successful read of a row with a type (b) clock
        clears exactly the clock it observed."""
        now = self._clock()
        async with self._sessions() as session:
            rows = await repo.unsettled_orders(session)
        read = _Read()
        min_qty: dict[str, Decimal] = {}
        for row in rows:
            if row.incident is not None:
                read.incidents[row.id] = row.incident
                continue
            broker_id = row.broker_order_id
            if broker_id is None:
                if row.status in _LOCAL_FINAL:
                    read.updates.append(
                        OrderUpdate(order_row_id=row.id, status=row.status, filled_qty=Decimal(0))
                    )
                else:  # pending / unknown without an id: the resolver's type (a)
                    read.candidates.append((row, "a"))
                continue
            try:
                read.updates.append(await self._read_order(row, broker_id, min_qty, deadline))
            except OrderNotFound:  # the broker no longer knows the id: type (b)
                log.exception("risk.order_sync_failed", order_row=row.id)
                read.failed.append(row.id)
                read.candidates.append((row, "b"))
            except Exception:  # the order is known, the read failed: not resolution
                log.exception("risk.order_sync_failed", order_row=row.id)
                read.failed.append(row.id)
            else:
                if row.resolve_since is not None:
                    read.resolutions.append(
                        OrderResolution(
                            order_row_id=row.id,
                            action="clear_clock",
                            at=now,
                            clock=row.resolve_since,
                        )
                    )
        return read

    async def _resolve(self, read: _Read, deadline: float) -> SyncResult:
        """Decide each candidate (module docstring). Writes nothing: the decisions travel in
        `SyncResult.resolutions` and the ledger applies each conditionally."""
        now = self._clock()  # once: a type (b) incident must carry the clock it started
        jid = await self._journal_id()
        live = {row.id for row, _ in read.candidates}  # forget rows that left resolution
        self._last_outcome = {k: v for k, v in self._last_outcome.items() if k in live}
        resolutions = read.resolutions
        incidents = dict(read.incidents)
        failed = read.failed
        if jid is None and read.candidates:  # a journal fault, not the rows': resolve nothing
            log.error("risk.journal_id_missing")
            read.candidates.clear()
        for row, kind in read.candidates:
            if kind == "a" and row.id in self._in_flight:  # its submit is still running
                continue
            since = now
            if kind == "b":
                if row.resolve_since is None:
                    resolutions.append(
                        OrderResolution(order_row_id=row.id, action="start_clock", at=since)
                    )
                else:
                    since = row.resolve_since
            clock = since if kind == "b" else None
            reason = await self._decide(row, kind, jid or "", since, deadline, now, resolutions)
            if reason is None:
                continue
            resolutions.append(
                OrderResolution(
                    order_row_id=row.id, action="incident", at=now, reason=reason, clock=clock
                )
            )
            incidents[row.id] = reason
            # D20.5: the incident itself blocks only its own symbol (after a restart the
            # skipped cash check still blocks every symbol, PLAN D20 note 2026-11-11)
            if kind == "b":
                failed.remove(row.id)
        self._remind(incidents)
        return SyncResult(
            updates=tuple(read.updates), failed=tuple(failed), resolutions=tuple(resolutions)
        )

    async def _decide(
        self,
        row: Order,
        kind: Literal["a", "b"],
        jid: str,
        since: datetime,
        deadline: float,
        now: datetime,
        resolutions: list[OrderResolution],
    ) -> IncidentReason | None:
        """One candidate: appends a link, or returns the incident reason (None = wait)."""
        cid = row.client_order_id
        if cid is None or not owns(cid, jid):
            return "no_ownership_proof"
        outcome: IncidentReason | None = None  # None: not looked up this pass
        if deadline - self._monotonic() > 0:
            try:
                view = await self._within(
                    deadline, partial(self._broker.get_order_by_client_id, cid)
                )
            except Exception:  # "don't know", never a zero match
                log.warning("risk.order_lookup_failed", order_row=row.id, exc_info=True)
                outcome = "lookup_failed"
            else:
                if view is None:
                    outcome = "not_found"
                elif view.client_order_id != cid or mismatches(
                    view,
                    symbol=row.symbol or "",
                    side=row.side,
                    qty=row.qty,
                    submitted_at=row.submitted_at,
                    broker_order_id=row.broker_order_id if kind == "b" else None,
                ):
                    log.error(
                        "risk.order_lookup_mismatch", order_row=row.id, order_id=view.order_id
                    )
                    return "client_id_mismatch"
                elif kind == "a":
                    resolutions.append(
                        OrderResolution(
                            order_row_id=row.id,
                            action="link",
                            at=now,
                            broker_order_id=view.order_id,
                        )
                    )
                    return None
                else:  # type (b): the broker knows the order but cannot read it (3.7)
                    outcome = "lookup_inconsistent"
            self._last_outcome[row.id] = outcome
        start = row.submitted_at if kind == "a" else since
        if now >= start + self._resolution_timeout:
            return outcome or self._last_outcome.get(row.id, "lookup_failed")
        expected = (
            row.submitted_at + timedelta(seconds=self._io_timeout) + self._visibility_allowance
        )
        (log.debug if now < expected else log.info)(
            "risk.order_resolve_pending", order_row=row.id, kind=kind, outcome=outcome
        )
        return None

    def _remind(self, incidents: dict[int, str]) -> None:
        """`risk.order_incident_open` every 60 s while any unsettled row is an incident."""
        if not incidents:
            return
        last = self._incident_logged_at
        if last is not None and self._monotonic() - last < _INCIDENT_REMINDER_S:
            return
        self._incident_logged_at = self._monotonic()
        log.error(
            "risk.order_incident_open",
            order_rows=[{"order_row": i, "reason": r} for i, r in sorted(incidents.items())],
        )

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
                    qty_available=p.qty_available,
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
        except DustRemainder as e:  # a full free sell below min_qty: report dust as a type (1.5c)
            log.warning("risk.exit_dust", order_row=row_id, qty=str(e.qty), min_qty=str(e.min_qty))
            await self._set_status(row_id, "rejected")
            return OrderResult(decision, row_id, "rejected", dust=True, min_qty=e.min_qty)
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
        """Reduce-only full exit (G1). No gate check: kill and pause never block it. Sells the
        free qty with `submit_order` as a market order whose client id (`at-{jid}-o{row}`)
        is stamped on the row when it is inserted; without a journal id it goes out with
        none (logged), because an exit must stay possible."""
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
            mode_matches = self._mode == self._broker.mode()
            decision = check_exit(request, held, mode_matches=mode_matches)
            await self._persist(request.cycle_id, "exit", decision, now)
            if decision.qty is None:
                log.warning("risk.exit_rejected", symbol=request.symbol, reason=decision.reasons[0])
                # A mode mismatch is a failure, not dust: say nothing about the holding then.
                position = next((p for p in held if p.symbol == request.symbol), None)
                if not mode_matches:
                    return OrderResult(decision)
                if position is None:
                    return OrderResult(decision, no_position=True)
                if not position.is_dust:  # open sells reserve the whole holding: a failure
                    return OrderResult(decision)
                return OrderResult(decision, dust=True, min_qty=position.min_qty)
            jid = await self._journal_id()
            if jid is None:  # reduce-only exits stay possible; the row just has no client id
                log.error("risk.journal_id_missing", symbol=request.symbol)
            claimed: list[int] = []  # the row id this call put into `_in_flight`
            try:
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
                        external=request.external,
                        client_order_id_for=None if jid is None else partial(exit_client_id, jid),
                    )
                    self._in_flight.add(row.id)  # inside the block: see `_in_flight`
                    claimed.append(row.id)
                sell = BrokerOrder(
                    symbol=request.symbol,
                    side="sell",
                    qty=decision.qty,
                    type="market",
                    time_in_force="gtc",
                    client_order_id=row.client_order_id,
                )
                return await self._submit(
                    row.id, decision, deadline, lambda: self._broker.submit_order(sell)
                )
            finally:
                self._in_flight.difference_update(claimed)

    async def cancel_open_entries(self, exclude: Collection[int] = ()) -> CancelResult:
        """Cancel unsettled, non-final entry (buy) orders at the broker, under the exit lock
        with its own budget. `exclude` skips rows whose cancel was already accepted. Rows
        without a broker id are `unresolved`. Never touches sells or order rows."""
        async with self._exit_lock:
            deadline = self._monotonic() + self._io_timeout
            async with self._sessions() as session:
                rows = await repo.open_entry_orders(session)
            requested: list[int] = []
            failed: list[int] = []
            unresolved: list[int] = []
            for row in rows:
                if row.id in exclude or row.status in FINAL:
                    continue
                if row.broker_order_id is None:
                    unresolved.append(row.id)
                    continue
                try:
                    await self._within(
                        deadline, partial(self._broker.cancel_order, row.broker_order_id)
                    )
                except Exception:
                    log.exception("risk.cancel_failed", order_row=row.id, symbol=row.symbol)
                    failed.append(row.id)
                else:
                    log.info("risk.entry_cancel_requested", order_row=row.id, symbol=row.symbol)
                    requested.append(row.id)
            return CancelResult(
                requested=tuple(requested), failed=tuple(failed), unresolved=tuple(unresolved)
            )

    async def sellable_positions(self) -> tuple[HeldPosition, ...]:
        """Non-dust broker holdings (unpriced) with their own budget; lock-free; raises on
        failure."""
        deadline = self._monotonic() + self._io_timeout
        return tuple(p for p in await self._broker_positions(deadline) if not p.is_dust)

    # --- equity snapshots ------------------------------------------------------------

    async def snapshot_equity(self) -> Account:
        """Called every `snapshot_interval_s` by `run core`'s supervised `risk.snapshot` loop. Its
        broker calls have their own `io_timeout_s` budget; journal writes are never cancelled.
        Lock-free on purpose: it must never delay an exit. Returns the account it read, so
        `run core` can feed it to the ledger's cash check without a second broker call."""
        deadline = self._monotonic() + self._io_timeout  # its own budget; no lock shared
        account = await self._within(deadline, self._broker.get_account)
        now = self._clock()  # receipt time stamps the row (spec 2026-11-09 3.1)
        try:
            held = await self._held_positions(deadline)
        except Exception:  # the equity (and a peak) is still recorded, exposure unknown
            await self._write_snapshot(now, account.equity, account.cash, None)
            self.last_snapshot_at = self._monotonic()
            raise
        await self._write_snapshot(now, account.equity, account.cash, held)
        self.last_snapshot_at = self._monotonic()
        return account

    async def _write_snapshot(
        self, now: datetime, equity: Decimal, cash: Decimal, held: tuple[HeldPosition, ...] | None
    ) -> None:
        """Append one row stamped `now` (the caller's receipt time of the account read);
        `held=None` (positions not read) stores exposure NULL. `daily_pnl` is informational
        (`repo.day_start_equity`, with its fallback); no limit reads it."""
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
