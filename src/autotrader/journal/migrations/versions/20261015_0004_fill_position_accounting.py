"""fill/position accounting (WP 1.5b, spec 2026-10-15-fill-position-accounting-design.md)

Fill identity and fee asset, order settlement, position dust/initial risk, and the
per-symbol dust balance (D17).

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-15 20:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import autotrader.journal.types

revision: str = "0004"
down_revision: str | Sequence[str] | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

Money = autotrader.journal.types.Money
UTCDateTime = autotrader.journal.types.UTCDateTime


def upgrade() -> None:
    # broker_fill_id is NOT NULL and nothing wrote fills before WP 1.5b: refuse instead of
    # inventing ids for rows that should not exist.
    if op.get_bind().execute(sa.text("SELECT COUNT(*) FROM fills")).scalar():
        raise RuntimeError("fills has rows: migration 0004 cannot invent broker_fill_id values")
    op.create_table(
        "symbol_dust",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("qty", Money(), nullable=False),
        sa.Column("updated_at", UTCDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("symbol", name=op.f("pk_symbol_dust")),
    )
    with op.batch_alter_table("fills") as batch:
        batch.add_column(sa.Column("broker_fill_id", sa.String(length=64), nullable=False))
        batch.add_column(sa.Column("side", sa.String(length=8), nullable=False))
        batch.add_column(sa.Column("fee_asset", sa.String(length=8), nullable=False))
        batch.create_unique_constraint(op.f("uq_fills_broker_fill_id"), ["broker_fill_id"])
        batch.create_check_constraint(op.f("ck_fills_side"), "side IN ('buy', 'sell')")
        batch.create_check_constraint(op.f("ck_fills_fee_asset"), "fee_asset IN ('base', 'quote')")
    with op.batch_alter_table("positions") as batch:
        batch.add_column(sa.Column("initial_risk", Money(), nullable=True))
        batch.add_column(sa.Column("dust_qty", Money(), nullable=True))
        batch.add_column(sa.Column("dust_consumed", Money(), server_default="0", nullable=False))
    with op.batch_alter_table("orders") as batch:
        batch.add_column(sa.Column("filled_qty", Money(), server_default="0", nullable=False))
        batch.add_column(sa.Column("settled_at", UTCDateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("position_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("exit_reason", sa.String(length=16), nullable=True))
        batch.create_foreign_key(
            op.f("fk_orders_position_id_positions"), "positions", ["position_id"], ["id"]
        )
    # Final rows that never reached the broker have no effects to apply: settled already.
    op.execute(
        sa.text(
            "UPDATE orders SET settled_at = updated_at"
            " WHERE broker_order_id IS NULL AND status IN ('not_submitted', 'rejected')"
        )
    )


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.drop_constraint(op.f("fk_orders_position_id_positions"), type_="foreignkey")
        batch.drop_column("exit_reason")
        batch.drop_column("position_id")
        batch.drop_column("settled_at")
        batch.drop_column("filled_qty")
    with op.batch_alter_table("positions") as batch:
        batch.drop_column("dust_consumed")
        batch.drop_column("dust_qty")
        batch.drop_column("initial_risk")
    with op.batch_alter_table("fills") as batch:
        batch.drop_constraint(op.f("ck_fills_fee_asset"), type_="check")
        batch.drop_constraint(op.f("ck_fills_side"), type_="check")
        batch.drop_constraint(op.f("uq_fills_broker_fill_id"), type_="unique")
        batch.drop_column("fee_asset")
        batch.drop_column("side")
        batch.drop_column("broker_fill_id")
    op.drop_table("symbol_dust")
