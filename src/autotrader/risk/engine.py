"""Risk Engine (SPEC §0.1, §4.5; spec 2026-10-11 §6). The only holder of the `Broker`.

`check()` gathers state under one lock, runs the pure `check_entry`, persists the
decision, applies a pause side effect on rejection, writes the order row, asks the
control gate again right before submitting, then submits. `exit()` (Task 7) is
reduce-only and ignores kill/pause. Any failure while gathering rejects (fail closed).
"""

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

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
from autotrader.risk.checks import check_entry
from autotrader.risk.types import (
    EntryIntent,
    HeldPosition,
    MarketSnapshot,
    OpenEntry,
    Pause,
    PortfolioState,
    RiskDecision,
)

log = structlog.get_logger(__name__)
HOUR = timedelta(hours=1)


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
        self._lock = asyncio.Lock()

    # --- entries ---------------------------------------------------------------------

    async def check(self, intent: EntryIntent) -> OrderResult:
        async with self._lock:
            now = self._clock()
            try:
                control, portfolio, market = await self._gather_entry(intent.symbol, now)
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
            final = await self._gate.status(self._clock())
            if not final.entries_allowed:
                await self._set_status(row.id, "not_submitted")
                log.warning("risk.entry_not_submitted", reasons=list(final.reasons))
                return OrderResult(decision, row.id, "not_submitted")
            return await self._submit(row.id, decision, lambda: self._broker.submit_order(order))

    async def _gather_entry(
        self, symbol: str, now: datetime
    ) -> tuple[ControlStatus, PortfolioState, MarketSnapshot]:
        control = await self._gate.status(now)
        # Refresh our own entries BEFORE reading positions: a fill the refresh observes
        # is then already in the positions read below (never neither-position-nor-entry).
        open_rows = await self._refresh_open_entries()
        account = await self._broker.get_account()
        held = await self._held_positions()
        rules = await self._broker.asset_rules(symbol)
        fees = self._broker.fee_schedule()
        async with self._sessions() as session:
            entries_hour = await repo.count_entry_orders(session, now - HOUR, inclusive=False)
            entries_day = await repo.count_entry_orders(
                session, day_start(now, self._tz), inclusive=True
            )
            baseline = await repo.day_start_equity(session, day_start(now, self._tz))
            latest = await repo.latest_snapshot(session)
            trades_n, wins = await repo.strategy_trade_stats(session, self._strategy_id)
        if latest is None:  # first check ever: this snapshot is the baseline
            await self._write_snapshot(now, account.equity, account.cash, held)
            baseline = account.equity
        hwm = max(latest.hwm if latest else account.equity, account.equity)
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

    async def _refresh_open_entries(self) -> list[Order]:
        """Order status sync for our own acked entries (WP 1.6 adds the fill stream):
        without it an IOC entry would stay `new` and block its symbol forever. No DB
        session is held open across the broker calls."""
        async with self._sessions() as session:
            rows = await repo.open_entry_orders(session)
        still_open: list[Order] = []
        changed: list[tuple[int, str]] = []
        for row in rows:
            if row.status == "new" and row.broker_order_id is not None:
                status = await self._broker.get_order(row.broker_order_id)
                if status.status != "new":
                    changed.append((row.id, status.status))
                    continue
            still_open.append(row)
        if changed:
            async with self._sessions() as session:
                for row_id, new_status in changed:
                    await repo.update_order(session, row_id, status=new_status, now=self._clock())
        return still_open

    async def _held_positions(self) -> tuple[HeldPosition, ...]:
        held = []
        for p in await self._broker.get_positions():
            price = _dec(self._market.last_price(p.symbol)) or p.avg_entry_price
            rules = await self._broker.asset_rules(p.symbol)
            held.append(
                HeldPosition(
                    symbol=p.symbol, qty=p.qty, market_value=p.qty * price, min_qty=rules.min_qty
                )
            )
        return tuple(held)

    async def _submit(
        self, row_id: int, decision: RiskDecision, call: Callable[[], Awaitable[OrderAck]]
    ) -> OrderResult:
        try:
            ack = await call()
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

    # --- equity snapshots ------------------------------------------------------------

    async def snapshot_equity(self) -> None:
        """Called every 60 s by `run core`. Lock-free on purpose: it must never delay
        an exit."""
        now = self._clock()
        account = await self._broker.get_account()
        await self._write_snapshot(now, account.equity, account.cash, await self._held_positions())

    async def _write_snapshot(
        self, now: datetime, equity: Decimal, cash: Decimal, held: tuple[HeldPosition, ...]
    ) -> None:
        async with self._sessions() as session:
            latest = await repo.latest_snapshot(session)
            baseline = await repo.day_start_equity(session, day_start(now, self._tz))
            hwm = max(latest.hwm if latest else equity, equity)
            await repo.insert_equity_snapshot(
                session,
                ts=now,
                equity=equity,
                cash=cash,
                exposure=sum((p.market_value for p in held), Decimal(0)),
                hwm=hwm,
                daily_pnl=equity - (baseline if baseline is not None else equity),
            )
