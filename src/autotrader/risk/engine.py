"""Risk Engine (SPEC §0.1, §4.5; spec 2026-10-11 §6). The only holder of the `Broker`.

`check()` gathers state under the entry lock, runs the pure `check_entry`, persists the
decision, applies a pause side effect on rejection, writes the order row, asks the
control gate again right before submitting, then submits. `exit()` is
reduce-only and ignores kill/pause. Any failure while gathering rejects (fail closed).

Two locks: entries serialise on one, exits on another, and an exit never waits for an
entry (a stalled journal call inside `check()` cannot block a stop-out). That is safe
because an entry that runs beside an exit still sees the position being exited and
rejects or over-counts it (conservative), and a late IOC fill after a kill-flatten can
happen with or without a shared lock (fills are broker-side); the Position Manager handles
it (WP 1.5).

Each locked section has ONE budget of `io_timeout_s`, measured from taking its lock and
shared by its network/gate awaits only (gate.status, get_account, get_positions, each
asset_rules and get_order, submit/close); each is wrapped in `_within`. Journal reads and
writes (every `self._sessions()` block, `_write_snapshot`) run outside any `wait_for`, so
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

from autotrader.broker.base import Broker, Mode, OrderAck, OrderRejected
from autotrader.core.config import StrategyProfile
from autotrader.core.indicators import compute_features
from autotrader.core.kill import PAUSED_KEY, PAUSED_UNTIL_KEY, ControlStatus
from autotrader.core.tradingday import day_start, next_day_start
from autotrader.core.types import Bar, Quote
from autotrader.journal import repo
from autotrader.journal.models import Order
from autotrader.risk.checks import check_entry, check_exit
from autotrader.risk.types import (
    EntryIntent,
    ExitRequest,
    HeldPosition,
    MarketSnapshot,
    OpenEntry,
    Pause,
    PortfolioState,
    RiskDecision,
)

log = structlog.get_logger(__name__)
HOUR = timedelta(hours=1)
T = TypeVar("T")


class RiskMarketView(Protocol):
    def history(self, symbol: str, timeframe: str) -> list[Bar]: ...
    def quote(self, symbol: str) -> Quote | None: ...
    def last_price(self, symbol: str) -> float | None: ...
    def is_stale(self, symbol: str) -> bool: ...
    def spread_bps(self, symbol: str) -> float | None: ...


class Gate(Protocol):
    async def status(self, now: datetime | None = None) -> ControlStatus: ...


@dataclass(frozen=True)
class OrderResult:
    decision: RiskDecision
    order_row_id: int | None = None
    status: str | None = None
    broker_order_id: str | None = None


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
        self._tz = trading_day_tz
        self._clock = clock
        self._io_timeout = io_timeout_s
        self._monotonic = monotonic  # the budget clock (injectable for tests)
        self._entry_lock = asyncio.Lock()  # check(): two entries never pass check 5 together
        self._exit_lock = asyncio.Lock()  # exit(): never waits for an entry

    # --- entries ---------------------------------------------------------------------

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
        control = await self._within(deadline, lambda: self._gate.status(now))
        # Refresh our own entries BEFORE reading positions: a fill the refresh observes
        # is then already in the positions read below (never neither-position-nor-entry).
        open_rows = await self._refresh_open_entries(deadline)
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
            # Re-read: the snapshot task may have committed a higher peak while this check
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
                # still counts toward max_positions (conservative)
                OpenEntry(symbol=o.symbol or "", notional=o.qty * (o.limit_price or Decimal(0)))
                for o in open_rows
            ),
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
            spread_bps=_dec(self._market.spread_bps(symbol)),
            atr=_dec(atr),
            stale=self._market.is_stale(symbol),
            rules=rules,
            fees=fees,
        )
        return control, portfolio, market

    async def _refresh_open_entries(self, deadline: float) -> list[Order]:
        """Order status sync for our own acked entries (WP 1.6 adds the fill stream):
        without it an IOC entry would stay `new` and block its symbol forever. No DB
        session is held open across the broker calls."""
        async with self._sessions() as session:
            rows = await repo.open_entry_orders(session)
        still_open: list[Order] = []
        changed: list[tuple[int, str]] = []
        for row in rows:
            if row.status == "new" and row.broker_order_id is not None:
                get_order = partial(self._broker.get_order, row.broker_order_id)
                status = await self._within(deadline, get_order)
                if status.status != "new":
                    changed.append((row.id, status.status))
                    continue
            still_open.append(row)
        if changed:
            async with self._sessions() as session:
                for row_id, new_status in changed:
                    await repo.update_order(session, row_id, status=new_status, now=self._clock())
        return still_open

    async def _held_positions(
        self, deadline: float, symbol: str | None = None
    ) -> tuple[HeldPosition, ...]:
        """Held positions with prices and rules. `symbol` narrows the (per-symbol REST)
        lookups to one asset, so an exit depends on nothing unrelated. `market_value` is
        None without a fresh feed price (never the cost basis): check 8 then rejects."""
        held = []
        for p in await self._within(deadline, self._broker.get_positions):
            if symbol is not None and p.symbol != symbol:
                continue
            price = _dec(self._market.last_price(p.symbol))
            fresh = price is not None and not self._market.is_stale(p.symbol)
            rules = await self._within(deadline, partial(self._broker.asset_rules, p.symbol))
            held.append(
                HeldPosition(
                    symbol=p.symbol,
                    qty=p.qty,
                    market_value=p.qty * price if fresh and price is not None else None,
                    avg_entry_price=p.avg_entry_price,
                    min_qty=rules.min_qty,
                )
            )
        return tuple(held)

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
        except OrderRejected as e:
            log.warning("risk.order_rejected", order_row=row_id, reason=str(e))
            await self._set_status(row_id, "rejected")
            return OrderResult(decision, row_id, "rejected")
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
                held = await self._held_positions(deadline, request.symbol)
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
                )
            return await self._submit(
                row.id, decision, deadline, lambda: self._broker.close_position(request.symbol)
            )

    # --- equity snapshots ------------------------------------------------------------

    async def snapshot_equity(self) -> None:
        """Called every 60 s by `run core`'s background snapshot task. Its broker calls
        have their own `io_timeout_s` budget; journal writes are never cancelled.
        Lock-free on purpose: it must never delay an exit."""
        deadline = self._monotonic() + self._io_timeout  # its own budget; no lock shared
        now = self._clock()
        account = await self._within(deadline, self._broker.get_account)
        try:
            held = await self._held_positions(deadline)
        except Exception:  # the equity (and a peak) is still recorded, exposure unknown
            await self._write_snapshot(now, account.equity, account.cash, None)
            raise
        await self._write_snapshot(now, account.equity, account.cash, held)

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
