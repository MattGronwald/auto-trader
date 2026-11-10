"""events.type index (WP 1.5d-1, spec 2026-11-09-pre-admission-hardening-design.md)

Lookup of the latest event of a given type (e.g. the last reconcile) must not scan the
append-only events table.

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-09 12:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | Sequence[str] | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("events", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_events_type"), ["type"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("events", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_events_type"))
