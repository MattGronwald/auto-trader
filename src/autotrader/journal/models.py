"""Journal tables (SPEC §7). Nothing is deleted; `events` is append-only.

Money and quantities use `Money` (exact Decimal), timestamps `UTCDateTime`. Bar prices
and model-estimated probabilities stay float (indicator / statistics inputs).
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import JSON, CheckConstraint, ForeignKey, MetaData, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from autotrader.journal.types import Money, UTCDateTime

# Named constraints: Alembic batch mode (SQLite ALTER) cannot touch unnamed ones.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    type_annotation_map = {  # noqa: RUF012 - SQLAlchemy reads this class attribute
        datetime: UTCDateTime,
        Decimal: Money,
        dict[str, Any]: JSON,
        list[Any]: JSON,
    }


Json = dict[str, Any]
Mode = String(8)


class Strategy(Base):
    __tablename__ = "strategies"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(64))
    yaml: Mapped[str] = mapped_column(Text)
    hash: Mapped[str] = mapped_column(String(64), unique=True)
    active_from: Mapped[datetime]
    active_to: Mapped[datetime | None]


class Cycle(Base):
    __tablename__ = "cycles"

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"))
    symbol: Mapped[str] = mapped_column(String(32))
    rule_id: Mapped[str] = mapped_column(String(64))
    signal_features: Mapped[Json]
    state: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(index=True)
    finished_at: Mapped[datetime | None]
    outcome: Mapped[str | None] = mapped_column(String(16))
    reject_reason: Mapped[str | None] = mapped_column(Text)


class AgentCall(Base):
    __tablename__ = "agent_calls"

    id: Mapped[int] = mapped_column(primary_key=True)
    # Nullable: the Journal Reviewer runs outside any cycle.
    cycle_id: Mapped[int | None] = mapped_column(ForeignKey("cycles.id"), index=True)
    agent: Mapped[str] = mapped_column(String(32))
    model: Mapped[str] = mapped_column(String(64))
    prompt_version: Mapped[str] = mapped_column(String(16))
    prompt_hash: Mapped[str] = mapped_column(String(64))
    input: Mapped[Json]
    # Nullable: a malformed/failed response is still recorded (and still costs money).
    output: Mapped[Json | None]
    tokens_in: Mapped[int]
    tokens_out: Mapped[int]
    cost_usd: Mapped[Decimal]
    latency_ms: Mapped[int]
    ts: Mapped[datetime]


class OrderIntent(Base):
    __tablename__ = "order_intents"

    id: Mapped[int] = mapped_column(primary_key=True)
    cycle_id: Mapped[int] = mapped_column(ForeignKey("cycles.id"))
    json: Mapped[Json]
    confidence: Mapped[float]
    thesis: Mapped[str] = mapped_column(Text)
    strongest_counter: Mapped[str] = mapped_column(Text)
    learnings_applied: Mapped[list[Any]]


class RiskDecision(Base):
    __tablename__ = "risk_decisions"
    __table_args__ = (CheckConstraint("kind IN ('entry', 'exit')", name="kind"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    # Nullable: reduce-only exits (G1) for kill/flatten are not tied to a cycle.
    cycle_id: Mapped[int | None] = mapped_column(ForeignKey("cycles.id"))
    kind: Mapped[str] = mapped_column(String(8))  # check() = entry, exit() = exit (G1)
    approved: Mapped[bool]
    reasons: Mapped[list[Any]]
    adjusted: Mapped[Json]
    computed_size: Mapped[Decimal | None]
    ts: Mapped[datetime]


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (CheckConstraint("mode IN ('paper', 'live')", name="mode"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    cycle_id: Mapped[int | None] = mapped_column(ForeignKey("cycles.id"))
    broker_order_id: Mapped[str | None] = mapped_column(String(64), unique=True)
    mode: Mapped[str] = mapped_column(Mode)
    type: Mapped[str] = mapped_column(String(16))
    side: Mapped[str] = mapped_column(String(8))
    qty: Mapped[Decimal]
    limit_price: Mapped[Decimal | None]
    stop_price: Mapped[Decimal | None]
    tp_price: Mapped[Decimal | None]
    status: Mapped[str] = mapped_column(String(16))
    submitted_at: Mapped[datetime]
    updated_at: Mapped[datetime]


class Fill(Base):
    __tablename__ = "fills"

    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"))
    qty: Mapped[Decimal]
    price: Mapped[Decimal]
    fee: Mapped[Decimal]
    ts: Mapped[datetime]


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(primary_key=True)
    cycle_id: Mapped[int | None] = mapped_column(ForeignKey("cycles.id"))
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(8))
    qty: Mapped[Decimal]
    entry_price: Mapped[Decimal]
    stop: Mapped[Decimal | None]
    tp: Mapped[Decimal | None]
    time_limit_at: Mapped[datetime | None]
    opened_at: Mapped[datetime]
    closed_at: Mapped[datetime | None] = mapped_column(index=True)
    exit_reason: Mapped[str | None] = mapped_column(String(32))
    realized_pnl: Mapped[Decimal | None]
    fees: Mapped[Decimal | None]
    mae: Mapped[Decimal | None]
    mfe: Mapped[Decimal | None]
    r_multiple: Mapped[Decimal | None]
    llm_cost_usd: Mapped[Decimal | None]
    benchmark_return: Mapped[Decimal | None]


class BarRow(Base):
    """Persisted 1m bars; other timeframes are derived (G5)."""

    __tablename__ = "bars"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    timeframe: Mapped[str] = mapped_column(String(8), primary_key=True)
    ts: Mapped[datetime] = mapped_column(primary_key=True)
    o: Mapped[float]
    h: Mapped[float]
    l: Mapped[float]  # noqa: E741 - SPEC column name
    c: Mapped[float]
    v: Mapped[float]


class EquitySnapshot(Base):
    __tablename__ = "equity_snapshots"

    ts: Mapped[datetime] = mapped_column(primary_key=True)
    equity: Mapped[Decimal]
    cash: Mapped[Decimal]
    exposure: Mapped[Decimal]
    hwm: Mapped[Decimal]
    daily_pnl: Mapped[Decimal]
    benchmark_equity: Mapped[Decimal | None]


class Event(Base):
    """Append-only. The repo layer exposes no update or delete."""

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True)
    ts: Mapped[datetime] = mapped_column(index=True)
    type: Mapped[str] = mapped_column(String(64))
    cycle_id: Mapped[int | None] = mapped_column(ForeignKey("cycles.id"))
    payload: Mapped[Json]


class Review(Base):
    __tablename__ = "reviews"

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"))
    period_start: Mapped[datetime]
    period_end: Mapped[datetime]
    report: Mapped[Json]
    cost_usd: Mapped[Decimal]
    ts: Mapped[datetime]


class Hypothesis(Base):
    __tablename__ = "hypotheses"
    __table_args__ = (
        CheckConstraint("status IN ('proposed', 'active', 'retired', 'rejected')", name="status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"))
    text: Mapped[str] = mapped_column(Text)
    kind: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16))
    created_by_review_id: Mapped[int | None] = mapped_column(ForeignKey("reviews.id"))
    evidence: Mapped[Json]
    trades_n: Mapped[int]
    win_rate: Mapped[float | None]
    avg_r: Mapped[float | None]
    p_value: Mapped[float | None]
    promoted_at: Mapped[datetime | None]
    retired_at: Mapped[datetime | None]


class ControlState(Base):
    """Keys: paused, paused_until, kill, mode."""

    __tablename__ = "control_state"

    key: Mapped[str] = mapped_column(String(32), primary_key=True)
    value: Mapped[Any] = mapped_column(JSON)
