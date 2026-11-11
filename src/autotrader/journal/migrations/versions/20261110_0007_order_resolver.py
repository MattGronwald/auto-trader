"""order resolver columns and the per-journal id (WP 1.5d-2)

Orders get the resolver state (`incident`, `incident_at`, `resolve_since`, `excess_external`),
fills the `external_qty` split, and `control_state` a once-written `journal_id` that makes
client order ids unique per journal.

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-10 12:00:00
"""

import secrets
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import autotrader.journal.types

revision: str = "0007"
down_revision: str | Sequence[str] | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

Money = autotrader.journal.types.Money
UTCDateTime = autotrader.journal.types.UTCDateTime


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.add_column(sa.Column("incident", sa.String(length=24), nullable=True))
        batch.add_column(sa.Column("incident_at", UTCDateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("resolve_since", UTCDateTime(timezone=True), nullable=True))
        batch.add_column(
            sa.Column("excess_external", sa.Boolean(), server_default=sa.false(), nullable=False)
        )
    with op.batch_alter_table("fills") as batch:
        batch.add_column(sa.Column("external_qty", Money(), server_default="0", nullable=False))
    # JSON-typed bind: Postgres (asyncpg) would type a plain text parameter as text.
    stmt = sa.text(
        "INSERT INTO control_state (key, value) SELECT 'journal_id', :value"
        " WHERE NOT EXISTS (SELECT 1 FROM control_state WHERE key = 'journal_id')"
    ).bindparams(sa.bindparam("value", value=secrets.token_hex(4), type_=sa.JSON))
    op.execute(stmt)


def downgrade() -> None:
    op.execute("DELETE FROM control_state WHERE key = 'journal_id'")
    with op.batch_alter_table("fills") as batch:
        batch.drop_column("external_qty")
    with op.batch_alter_table("orders") as batch:
        batch.drop_column("excess_external")
        batch.drop_column("resolve_since")
        batch.drop_column("incident_at")
        batch.drop_column("incident")
