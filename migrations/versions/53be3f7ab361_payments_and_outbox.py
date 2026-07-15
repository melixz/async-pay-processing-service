"""payments and outbox

Revision ID: 53be3f7ab361
Revises:
Create Date: 2026-07-15 16:45:52.889394

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "53be3f7ab361"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CURRENCY = postgresql.ENUM("RUB", "USD", "EUR", name="currency", create_type=False)
_PAYMENT_STATUS = postgresql.ENUM("pending", "succeeded", "failed", name="payment_status", create_type=False)


def upgrade() -> None:
    """Создать таблицы outbox и payments."""
    op.create_table(
        "outbox",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("aggregate_id", sa.UUID(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("routing_key", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    # Частичный индекс: relay сканирует только строки, ждущие публикации.
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )

    op.create_table(
        "payments",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("amount", sa.Numeric(precision=20, scale=2), nullable=False),
        sa.Column("currency", sa.Enum("RUB", "USD", "EUR", name="currency"), nullable=False),
        sa.Column("description", sa.String(length=1024), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Enum("pending", "succeeded", "failed", name="payment_status"), nullable=False),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_index(op.f("ix_payments_status"), "payments", ["status"], unique=False)


def downgrade() -> None:
    """Удалить обе таблицы и принадлежащие им enum-типы."""
    op.drop_index(op.f("ix_payments_status"), table_name="payments")
    op.drop_table("payments")
    op.drop_index("ix_outbox_unpublished", table_name="outbox", postgresql_where=sa.text("published_at IS NULL"))
    op.drop_table("outbox")
    _PAYMENT_STATUS.drop(op.get_bind())
    _CURRENCY.drop(op.get_bind())
