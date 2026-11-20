import hashlib
import hmac
import json
import re
import time
from datetime import datetime, timezone
from typing import Literal

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    status,
)
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from sqlalchemy import case
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db, settings
from app.models import DeliveryReceipt, Notification, OutboxEvent
from app.schemas import DeliveryReceiptRequest, NotificationCreate
from app.routers.push import router as push_router
from app.metrics import PrometheusMiddleware

app = FastAPI(
    title="NotifyFlow API",
    description="An asynchronous notification delivery platform",
    version="1.0.0",
)

# Collect HTTP request metrics.
app.add_middleware(PrometheusMiddleware)

app.include_router(push_router)

TERMINAL_NOTIFICATION_STATUSES = {"delivered", "failed"}


def serialize_notification(notification: Notification):
    return {
        "id": notification.id,
        "recipient": notification.recipient,
        "message": notification.message,
        "status": notification.status,
        "priority": notification.priority,
        "created_at": notification.created_at,
    }


def matches_request(
    existing: Notification,
    notification: NotificationCreate,
) -> bool:
    return (
        existing.recipient == notification.to
        and existing.message == notification.message
        and existing.priority == notification.priority
    )


def is_idempotency_conflict(exc: IntegrityError) -> bool:
    diag = getattr(exc.orig, "diag", None)
    constraint_name = getattr(diag, "constraint_name", None)

    return constraint_name in {
        "uq_notifications_idempotency_key",
        "notifications_idempotency_key_key",
    }


def calculate_receipt_fingerprint(
    receipt: DeliveryReceiptRequest,
    provider_name: str | None,
) -> str:
    """Create a stable fingerprint excluding webhook timestamp/signature."""
    canonical_payload = {
        "provider_name": provider_name,
        "provider_message_id": receipt.provider_message_id,
        "status": receipt.status,
        "failure_reason": receipt.failure_reason,
    }
    canonical_bytes = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    return hashlib.sha256(canonical_bytes).hexdigest()


@app.get("/")
def root():
    return {
        "message": "Welcome to NotifyFlow",
        "status": "running",
    }


@app.get("/health")
def health_check():
    return {"status": "healthy"}


# ---------------------------------------------------------
# Prometheus metrics endpoint
# ---------------------------------------------------------

@app.get("/metrics", include_in_schema=False)
def metrics():
    """Expose application metrics in Prometheus format."""

    return Response(
        content=generate_latest(),
        headers={
            "Content-Type": CONTENT_TYPE_LATEST,
        },
    )


@app.post("/notifications", status_code=status.HTTP_202_ACCEPTED)
def create_notification(
    notification: NotificationCreate,
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
    ),
    db: Session = Depends(get_db),
):
    # Step 1: Check whether this idempotency key already exists.
    if idempotency_key is not None:
        existing = (
            db.query(Notification)
            .filter(Notification.idempotency_key == idempotency_key)
            .first()
        )

        if existing is not None:
            if not matches_request(existing, notification):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Idempotency key already used "
                        "with a different request payload"
                    ),
                )

            return serialize_notification(existing)

    # Step 2: Create the notification.
    new_notification = Notification(
        recipient=notification.to,
        message=notification.message,
        priority=notification.priority,
        idempotency_key=idempotency_key,
    )

    try:
        db.add(new_notification)

        # Step 3: Insert notification without committing.
        db.flush()

        # Step 4: Create the corresponding outbox event.
        outbox_event = OutboxEvent(
            notification_id=new_notification.id,
            event_type="notification.created",
            payload={
                "notification_id": new_notification.id,
                "recipient": new_notification.recipient,
                "message": new_notification.message,
                "priority": new_notification.priority,
            },
        )

        db.add(outbox_event)

        # Step 5: Commit both records in one transaction.
        db.commit()

    except IntegrityError as exc:
        db.rollback()

        if idempotency_key is None or not is_idempotency_conflict(exc):
            raise

        existing = (
            db.query(Notification)
            .filter(Notification.idempotency_key == idempotency_key)
            .first()
        )

        if existing is None:
            raise

        if not matches_request(existing, notification):
            raise HTTPException(
                status_code=409,
                detail=(
                    "Idempotency key already used "
                    "with a different request payload"
                ),
            )

        return serialize_notification(existing)

    return serialize_notification(new_notification)


@app.get("/notifications")
def list_notifications(
    limit: int = Query(default=10, ge=1, le=100),
    status_filter: Literal[
        "queued", "processing", "sent", "delivered", "failed"
    ] | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
):
    query = db.query(Notification)

    if status_filter is not None:
        query = query.filter(Notification.status == status_filter)

    priority_order = case(
        (Notification.priority == "high", 1),
        (Notification.priority == "normal", 2),
        (Notification.priority == "low", 3),
        else_=4,
    )

    notifications = (
        query
        .order_by(priority_order.asc(), Notification.id.desc())
        .limit(limit)
        .all()
    )

    return [serialize_notification(notification) for notification in notifications]


@app.get("/notifications/{notification_id}")
def get_notification(
    notification_id: int,
    db: Session = Depends(get_db),
):
    notification = (
        db.query(Notification)
        .filter(Notification.id == notification_id)
        .first()
    )

    if notification is None:
        raise HTTPException(
            status_code=404,
            detail="Notification not found",
        )

    return serialize_notification(notification)


async def verify_dlr_signature(
    request: Request,
    x_webhook_timestamp: str | None = Header(
        default=None,
        alias="X-Webhook-Timestamp",
    ),
    x_webhook_signature: str | None = Header(
        default=None,
        alias="X-Webhook-Signature",
    ),
) -> None:
    # Step 1: Ensure authentication headers exist.
    if not x_webhook_timestamp or not x_webhook_signature:
        raise HTTPException(
            status_code=401,
            detail="Missing webhook authentication headers",
        )

    # Step 2: Validate timestamp format.
    try:
        timestamp = int(x_webhook_timestamp)
    except ValueError:
        raise HTTPException(
            status_code=401,
            detail="Invalid webhook timestamp",
        )

    # Step 3: Reject stale or excessively future-dated requests.
    current_timestamp = int(time.time())
    tolerance = settings.DLR_WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS

    if abs(current_timestamp - timestamp) > tolerance:
        raise HTTPException(
            status_code=401,
            detail="Webhook timestamp expired",
        )

    # Step 4: Read the original request body.
    raw_body = await request.body()

    # Step 5: Recreate the exact payload the sender signed.
    signed_payload = x_webhook_timestamp.encode("utf-8") + b"." + raw_body

    # Step 6: Calculate the expected HMAC-SHA256 signature.
    secret = settings.DLR_WEBHOOK_SECRET.get_secret_value()
    digest = hmac.new(
        secret.encode("utf-8"),
        signed_payload,
        hashlib.sha256,
    ).hexdigest()
    expected_signature = f"sha256={digest}"

    # Step 7: Reject malformed signatures before constant-time comparison.
    if re.fullmatch(r"sha256=[0-9a-f]{64}", x_webhook_signature) is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid webhook signature",
        )

    if not hmac.compare_digest(expected_signature, x_webhook_signature):
        raise HTTPException(
            status_code=401,
            detail="Invalid webhook signature",
        )


@app.post("/webhooks/dlr")
def delivery_receipt(
    receipt: DeliveryReceiptRequest,
    db: Session = Depends(get_db),
    _: None = Depends(verify_dlr_signature),
):
    """Authenticate, deduplicate, audit, and apply a provider delivery receipt."""
    try:
        # Lock the notification so concurrent callbacks for it are serialized.
        notification = (
            db.query(Notification)
            .filter(
                Notification.provider_message_id == receipt.provider_message_id
            )
            .with_for_update()
            .first()
        )

        if notification is None:
            raise HTTPException(
                status_code=404,
                detail="Notification not found for this provider message ID",
            )

        # A callback before provider acceptance is not recorded as processed.
        # Otherwise, a later legitimate retry could be suppressed as a duplicate.
        if notification.status not in TERMINAL_NOTIFICATION_STATUSES and notification.status != "sent":
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot process delivery receipt "
                    f"for notification in status: {notification.status}"
                ),
            )

        fingerprint = calculate_receipt_fingerprint(
            receipt, notification.provider_name
        )
        receipt_payload = {
            "provider_message_id": receipt.provider_message_id,
            "status": receipt.status,
            "failure_reason": receipt.failure_reason,
        }

        # Insert the receipt idempotently. The database unique constraint is the
        # final protection against concurrent or repeated deliveries.
        insert_statement = (
            pg_insert(DeliveryReceipt)
            .values(
                notification_id=notification.id,
                provider_name=notification.provider_name,
                provider_message_id=receipt.provider_message_id,
                status=receipt.status,
                receipt_fingerprint=fingerprint,
                failure_reason=receipt.failure_reason,
                payload=receipt_payload,
            )
            .on_conflict_do_nothing(
                constraint="uq_delivery_receipts_fingerprint"
            )
            .returning(DeliveryReceipt.id)
        )

        inserted_receipt_id = db.execute(
            insert_statement
        ).scalar_one_or_none()

        if inserted_receipt_id is None:
            db.commit()
            return {
                "message": "Duplicate delivery receipt ignored",
                "notification_id": notification.id,
                "status": notification.status,
            }

        # A new receipt for an already-terminal notification is retained for
        # audit, but it cannot change the established terminal status.
        if notification.status in TERMINAL_NOTIFICATION_STATUSES:
            existing_status = notification.status
            db.commit()

            if existing_status == receipt.status:
                return {
                    "message": "Duplicate delivery receipt ignored",
                    "notification_id": notification.id,
                    "status": existing_status,
                }

            raise HTTPException(
                status_code=409,
                detail=(
                    f"Notification already has terminal status: "
                    f"{existing_status}"
                ),
            )

        # Apply the final receipt to a notification that is currently sent.
        current_time = datetime.now(timezone.utc)
        notification.provider_status = receipt.status

        if receipt.status == "delivered":
            notification.status = "delivered"
            notification.delivered_at = current_time
            notification.failure_reason = None

        elif receipt.status == "failed":
            notification.status = "failed"
            notification.failed_at = current_time
            notification.failure_reason = (
                receipt.failure_reason or "Delivery failed"
            )

        # Commit receipt history and notification transition atomically.
        db.commit()
        db.refresh(notification)

        return {
            "message": "Delivery receipt processed successfully",
            "notification_id": notification.id,
            "status": notification.status,
            "provider_status": notification.provider_status,
            "receipt_id": inserted_receipt_id,
        }

    except HTTPException:
        # Do not roll back the shared test session's outer transaction.
        # In production, get_db() closes the request-scoped session, which
        # releases any uncommitted read transaction and row locks.
        # Expected HTTP errors are raised before any uncommitted DLR writes,
        # except conflicting terminal receipts, which are committed above.
        raise
    except Exception:
        db.rollback()
        raise
