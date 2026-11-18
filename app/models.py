
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    recipient: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    message: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="queued",
        server_default=text("'queued'"),
    )

    priority: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        default="normal",
        server_default=text("'normal'"),
    )

    idempotency_key: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # SMS provider information
    provider_name: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
    )

    provider_message_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
    )

    provider_status: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
    )

    provider_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )

    # Notification delivery timestamps
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    failure_reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    __table_args__ = (
        Index(
            "ix_outbox_events_published_created",
            "published_at",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    notification_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("notifications.id"),
        nullable=False,
    )

    event_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )

    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )


class ConsumerInbox(Base):
    __tablename__ = "consumer_inbox"

    event_id: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
    )

    notification_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("notifications.id"),
        nullable=False,
    )

    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class DeliveryReceipt(Base):
    __tablename__ = "delivery_receipts"

    __table_args__ = (
        UniqueConstraint(
            "receipt_fingerprint",
            name="uq_delivery_receipts_fingerprint",
        ),
        Index(
            "ix_delivery_receipts_notification_received",
            "notification_id",
            "received_at",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    notification_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("notifications.id"),
        nullable=False,
    )

    provider_name: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
    )

    provider_message_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    receipt_fingerprint: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )

    failure_reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class PushSubscription(Base):
    __tablename__ = "push_subscriptions"

    __table_args__ = (
        UniqueConstraint(
            "endpoint",
            name="uq_push_subscriptions_endpoint",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    endpoint: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    p256dh_key: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    auth_key: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    
    expired_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

class PushNotification(Base):
    __tablename__ = "push_notifications"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    title: Mapped[str] = mapped_column(
        String(120),
        nullable=False,
    )

    body: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    target_url: Mapped[str] = mapped_column(
        String(2048),
        nullable=False,
        default="/",
        server_default=text("'/'"),
    )

    status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="queued",
        server_default=text("'queued'"),
    )

    idempotency_key: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
    )

    failure_reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class PushOutboxEvent(Base):
    __tablename__ = "push_outbox_events"

    __table_args__ = (
        Index(
            "ix_push_outbox_published_created",
            "published_at",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    push_notification_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("push_notifications.id"),
        nullable=False,
        unique=True,
    )

    event_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )

    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )


class PushDelivery(Base):
    __tablename__ = "push_deliveries"

    __table_args__ = (
        UniqueConstraint(
            "push_notification_id",
            "push_subscription_id",
            name="uq_push_delivery_notification_subscription",
        ),
        Index(
            "ix_push_deliveries_notification_status",
            "push_notification_id",
            "status",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    push_notification_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("push_notifications.id"),
        nullable=False,
    )

    push_subscription_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("push_subscriptions.id"),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="queued",
        server_default=text("'queued'"),
    )

    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )

    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    push_status_code: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    accepted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
