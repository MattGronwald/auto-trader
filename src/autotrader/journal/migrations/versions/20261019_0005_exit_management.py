"""exit management (WP 1.5c, spec 2026-10-19-exit-management-design.md)

Durable exit state on positions (ATR at entry, trailing high/stop, latched trigger) and the
`external` flag on orders (a sell of a broker holding with no journal position).

Revision ID: 0005
Revises: 0004
Create Date: 2026-10-19 12:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import autotrader.journal.types

revision: str = "0005"
down_revision: str | Sequence[str] | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

Money = autotrader.journal.types.Money
UTCDateTime = autotrader.journal.types.UTCDateTime


def upgrade() -> None:
    with op.batch_alter_table("positions") as batch:
        batch.add_column(sa.Column("atr_at_entry", Money(), nullable=True))
        batch.add_column(sa.Column("trail_high", Money(), nullable=True))
        batch.add_column(sa.Column("trail_stop", Money(), nullable=True))
        batch.add_column(sa.Column("exit_trigger", sa.String(length=16), nullable=True))
        batch.add_column(sa.Column("exit_triggered_at", UTCDateTime(timezone=True), nullable=True))
    with op.batch_alter_table("orders") as batch:
        batch.add_column(
            sa.Column("external", sa.Boolean(), server_default=sa.false(), nullable=False)
        )


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.drop_column("external")
    with op.batch_alter_table("positions") as batch:
        batch.drop_column("exit_triggered_at")
        batch.drop_column("exit_trigger")
        batch.drop_column("trail_stop")
        batch.drop_column("trail_high")
        batch.drop_column("atr_at_entry")
