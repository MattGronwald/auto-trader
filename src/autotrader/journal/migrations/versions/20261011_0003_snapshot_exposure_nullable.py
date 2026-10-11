"""equity_snapshots.exposure nullable (PR #13 review round 2)

NULL = exposure unknown at that observation: the Risk Engine persists an equity peak as
soon as the broker account is read, before positions are read (and when they fail).

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-11 20:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import autotrader.journal.types

revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("equity_snapshots") as batch:
        batch.alter_column(
            "exposure", existing_type=autotrader.journal.types.Money(), nullable=True
        )


def downgrade() -> None:
    # NOT NULL again: unknown exposure becomes 0 (informational column, never a limit).
    op.execute(sa.text("UPDATE equity_snapshots SET exposure = '0' WHERE exposure IS NULL"))
    with op.batch_alter_table("equity_snapshots") as batch:
        batch.alter_column(
            "exposure", existing_type=autotrader.journal.types.Money(), nullable=False
        )
