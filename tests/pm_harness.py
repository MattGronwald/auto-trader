"""Position Manager test harness: SimBroker + RiskEngine + Ledger + a scripted quote view."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import AssetRules, BrokerOrder, FeeSchedule
from autotrader.broker.sim import SimBroker
from autotrader.core.bus import EventBus
from autotrader.core.config import AppConfig, StrategyProfile, load_profile
from autotrader.core.kill import ControlStatus
from autotrader.core.ledger import Ledger
from autotrader.core.position_manager import PositionManager
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.risk.engine import RiskEngine

REPO = Path(__file__).resolve().parents[1]
PROFILE: StrategyProfile = load_profile(REPO / "strategies" / "fast_momentum_v1.yaml")
BTC, ETH = "BTC/USD", "ETH/USD"
T0 = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
RULES = AssetRules(
    min_qty=D("0.0001"), qty_increment=D("0.0001"), min_notional=D("1"), price_increment=D("0.01")
)
QUOTE_FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"), buy_fee_asset="quote")
BASE_FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"), buy_fee_asset="base")


def app_config(
    *,
    kill_flatten: bool | None = None,
    stale_exit: bool | None = None,
    shutdown_flatten: bool = False,
) -> AppConfig:
    return AppConfig.model_validate(
        {
            "mode": "paper",
            "strategy": "s.yaml",
            "trading_day_tz": "UTC",
            "database": {"url": "sqlite+aiosqlite:///unused.db"},
            "control": {"kill_file": "KILL"},
            "kill": {"flatten": kill_flatten},
            "feed": {"stale_alert_s": 60, "stale_exit_s": 300, "stale_exit_enabled": stale_exit},
            "shutdown": {"flatten": shutdown_flatten, "timeout_s": 30},
        }
    )


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@dataclass
class View:
    """ExitMarketView: quotes, stale symbols and explicit price ages."""

    clock: Clock
    quotes: dict[str, Quote] = field(default_factory=dict)
    stale: set[str] = field(default_factory=set)
    ages: dict[str, float] = field(default_factory=dict)

    def quote(self, symbol: str) -> Quote | None:
        return self.quotes.get(symbol)

    def is_stale(self, symbol: str) -> bool:
        return symbol in self.stale

    def price_age_s(self, symbol: str) -> float | None:
        if symbol in self.ages:
            return self.ages[symbol]
        q = self.quotes.get(symbol)
        return None if q is None else (self.clock.now - q.ts).total_seconds()


@dataclass
class Controls:
    latest: ControlStatus | None = None


class OpenGate:
    async def status(self, now: datetime | None = None) -> ControlStatus:
        return ControlStatus(False, ())


class NoPrices:  # RiskMarketView: exits never read the feed
    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return []

    def quote(self, symbol: str) -> Quote | None:
        return None

    def last_price(self, symbol: str) -> float | None:
        return None

    def is_stale(self, symbol: str) -> bool:
        return True


class Harness:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        bus: EventBus,
        *,
        sim: SimBroker | None = None,
        config: AppConfig | None = None,
        profile: StrategyProfile = PROFILE,
        engine_cls: type[RiskEngine] = RiskEngine,
    ) -> None:
        self.sessions, self.bus = sessions, bus
        self.clock = Clock(T0)
        self.sim = sim or make_sim()
        self.minute = 0
        self.ledger = Ledger(bus, sessions, clock=self.clock)
        self.view = View(self.clock)
        self.controls = Controls()
        self.config = config or app_config()
        self.engine = engine_cls(
            self.sim,
            profile=profile,
            strategy_id=1,
            mode="paper",
            gate=OpenGate(),
            market=NoPrices(),
            sessions=sessions,
            ledger=self.ledger,
            clock=self.clock,
        )
        self.pm = self.new_pm(profile)

    def new_pm(self, profile: StrategyProfile = PROFILE) -> PositionManager:
        """A fresh manager on the same journal: a restart."""
        return PositionManager(
            self.engine,
            self.view,
            self.sessions,
            self.bus,
            self.controls,
            profile=profile,
            config=self.config,
            clock=self.clock,
        )

    def bar(self, symbol: str, price: float) -> None:
        """The next minute's bar: fills what is open for `symbol`."""
        self.minute += 1
        self.sim.process_bar(_bar(symbol, self.minute, price))

    def quote(self, symbol: str, bid: float, *, age_s: float = 0.0) -> None:
        ts = self.clock.now - timedelta(seconds=age_s)
        self.view.quotes[symbol] = Quote(symbol=symbol, ts=ts, bid=bid, ask=bid + 0.01)

    async def sync(self) -> None:
        await self.ledger.apply_all(await self.engine.sync_orders())

    async def open_position(
        self,
        symbol: str = BTC,
        qty: str = "1",
        *,
        stop: str | None = "95",
        tp: str | None = "110",
        atr: str | None = "2",
    ) -> int:
        """Journal an entry like check() does, fill it at 100 on the next bar, book it and
        return the position id. A FillPlan scripted before the call shapes the fill."""
        row_id, _ = await self.submit_entry(symbol, qty, limit=None, stop=stop, tp=tp, atr=atr)
        self.bar(symbol, 100.0)
        await self.sync()
        async with self.sessions() as s:
            row = await s.get(m.Order, row_id, populate_existing=True)
            if row is None or row.position_id is None:
                raise LookupError(row_id)
            return row.position_id

    async def submit_entry(
        self,
        symbol: str = BTC,
        qty: str = "1",
        *,
        limit: str | None = "50",
        ack: bool = True,
        stop: str | None = "95",
        tp: str | None = "110",
        atr: str | None = "2",
    ) -> tuple[int, str]:
        """An entry at the broker, not booked: a GTC limit buy at `limit` rests (never fills
        at 100); `limit=None` is a market buy that fills on the symbol's next bar. With
        `ack=False` the journal row stays `pending` without a broker id (an un-acked submit).
        Returns (order row id, broker order id)."""
        now = self.clock.now
        adjusted: dict[str, str] = {}
        if stop is not None:
            adjusted["initial_risk"] = str(D(qty) * (100 - D(stop)))
        if atr is not None:
            adjusted["atr"] = atr
        async with self.sessions() as s:
            strategy = await repo.register_strategy(s, PROFILE, "yaml", now=now)
            cycle = m.Cycle(
                strategy_id=strategy.id,
                symbol=symbol,
                rule_id="r",
                signal_features={},
                state="SUBMITTED",
                created_at=now,
            )
            s.add(cycle)
            await s.commit()
            await repo.record_risk_decision(
                s,
                cycle_id=cycle.id,
                kind="entry",
                approved=True,
                reasons=[],
                adjusted=adjusted,
                computed_size=D(qty),
                ts=now,
            )
        if limit is None:
            order = BrokerOrder(symbol=symbol, side="buy", qty=D(qty), type="market")
        else:
            order = BrokerOrder(
                symbol=symbol, side="buy", qty=D(qty), type="limit", limit_price=D(limit)
            )
        acked = await self.sim.submit_order(order)
        async with self.sessions() as s:
            row = await repo.insert_order(
                s,
                cycle_id=cycle.id,
                client_order_id=None,
                symbol=symbol,
                mode="paper",
                type_=order.type,
                side="buy",
                qty=D(qty),
                limit_price=order.limit_price,
                stop_price=None if stop is None else D(stop),
                tp_price=None if tp is None else D(tp),
                status="pending",
                now=now,
            )
            if ack:
                await repo.update_order(
                    s, row.id, status=acked.status, now=now, broker_order_id=acked.order_id
                )
        return row.id, acked.order_id

    async def position(self, position_id: int) -> m.Position:
        async with self.sessions() as s:
            row = await s.get(m.Position, position_id, populate_existing=True)
            if row is None:
                raise LookupError(position_id)
            return row

    async def sells(self, symbol: str = BTC) -> list[m.Order]:
        async with self.sessions() as s:
            rows = await s.scalars(
                select(m.Order)
                .where(m.Order.symbol == symbol, m.Order.side == "sell")
                .order_by(m.Order.id)
            )
            return list(rows)


def make_sim(fees: FeeSchedule = QUOTE_FEES, cls: type[SimBroker] = SimBroker) -> SimBroker:
    sim = cls(cash=D("100000"), fees=fees, slippage_bps=D(0), asset_rules={BTC: RULES, ETH: RULES})
    for symbol in (BTC, ETH):
        sim.process_bar(_bar(symbol, 0, 100.0))
    return sim


def _bar(symbol: str, minute: int, price: float) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe="1m",
        ts=T0 + timedelta(minutes=minute),
        open=price,
        high=price,
        low=price,
        close=price,
        volume=1.0,
    )


async def event_payloads(sessions: async_sessionmaker[AsyncSession], type_: str) -> list[Any]:
    async with sessions() as s:
        return [e.payload for e in await s.scalars(select(m.Event).where(m.Event.type == type_))]
