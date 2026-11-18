
"""add notification idempotency key

Revision ID: e4661eeeb4cc
Revises: 3c9ba9e74476
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e4661eeeb4cc"
down_revision: Union[str, Sequence[str], None] = "3c9ba9e74476"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "notifications",
        sa.Column(
            "idempotency_key",
            sa.String(length=255),
            nullable=True,
        ),
    )

    op.create_unique_constraint(
        "uq_notifications_idempotency_key",
        "notifications",
        ["idempotency_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_notifications_idempotency_key",
        "notifications",
        type_="unique",
    )

    op.drop_column(
        "notifications",
        "idempotency_key",
    )
