"""orders.client_order_id (WP 1.3, D14)

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-11 15:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.add_column(sa.Column("client_order_id", sa.String(length=64), nullable=True))
        batch.add_column(sa.Column("symbol", sa.String(length=32), nullable=True))
        batch.create_unique_constraint(op.f("uq_orders_client_order_id"), ["client_order_id"])


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.drop_constraint(op.f("uq_orders_client_order_id"), type_="unique")
        batch.drop_column("symbol")
        batch.drop_column("client_order_id")
