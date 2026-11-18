import hashlib
import hmac
import json
import time

import pytest

from app.database import settings
from app.models import DeliveryReceipt, Notification, OutboxEvent

def post_signed_dlr(
    client,
    payload,
    timestamp=None,
    signing_secret=None,
):
    """Send a DLR request signed with NotifyFlow's HMAC protocol."""
    timestamp = str(
        timestamp if timestamp is not None else int(time.time())
    )

    body = json.dumps(
        payload,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    secret = (
        signing_secret
        if signing_secret is not None
        else settings.DLR_WEBHOOK_SECRET.get_secret_value()
    )

    signed_payload = timestamp.encode("utf-8") + b"." + body

    signature = hmac.new(
        secret.encode("utf-8"),
        signed_payload,
        hashlib.sha256,
    ).hexdigest()

    return client.post(
        "/webhooks/dlr",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": f"sha256={signature}",
        },
    )


def test_health_check(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_invalid_notification_status(client):
    response = client.get(
        "/notifications",
        params={"status": "unknown"},
    )

    assert response.status_code == 422


def test_invalid_notification_limit(client):
    response = client.get(
        "/notifications",
        params={"limit": 101},
    )

    assert response.status_code == 422


def test_create_notification(client):
    response = client.post(
        "/notifications",
        json={
            "to": "+919876543210",
            "message": "Automated test notification",
        },
    )

    assert response.status_code == 202

    data = response.json()
    assert isinstance(data["id"], int)
    assert data["recipient"] == "+919876543210"
    assert data["message"] == "Automated test notification"
    assert data["status"] == "queued"
    assert data["priority"] == "normal"
    assert data["created_at"] is not None


def test_get_notification(client):
    create_response = client.post(
        "/notifications",
        json={
            "to": "+919876543210",
            "message": "Notification to retrieve",
        },
    )

    notification_id = create_response.json()["id"]

    response = client.get(f"/notifications/{notification_id}")

    assert response.status_code == 200
    assert response.json()["id"] == notification_id
    assert response.json()["message"] == "Notification to retrieve"
    assert response.json()["priority"] == "normal"


def test_get_notification_not_found(client):
    response = client.get("/notifications/99999999")

    assert response.status_code == 404
    assert response.json()["detail"] == "Notification not found"


def test_notification_status_filter(client):
    create_response = client.post(
        "/notifications",
        json={
            "to": "+919876543210",
            "message": "Test status filtering",
        },
    )

    assert create_response.status_code == 202

    response = client.get(
        "/notifications",
        params={"status": "queued"},
    )

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["status"] == "queued"

    response = client.get(
        "/notifications",
        params={"status": "delivered"},
    )

    assert response.status_code == 200
    assert response.json() == []


def test_create_notification_with_high_priority(client):
    response = client.post(
        "/notifications",
        json={
            "to": "+919876543210",
            "message": "Urgent OTP notification",
            "priority": "high",
        },
    )

    assert response.status_code == 202
    assert response.json()["priority"] == "high"

def test_invalid_notification_priority(client):
    response = client.post(
        "/notifications",
        json={
            "to": "+919876543210",
            "message": "Invalid priority test",
            "priority": "urgent",
        },
    )

    assert response.status_code == 422


def test_notification_priority_order(client):
    priorities = ["low", "normal", "high"]

    # Create notifications in low-to-high order
    for priority in priorities:
        response = client.post(
            "/notifications",
            json={
                "to": "+919876543210",
                "message": f"Test {priority} priority",
                "priority": priority,
            },
        )

        assert response.status_code == 202

    # Retrieve notifications
    response = client.get(
        "/notifications",
        params={"limit": 3},
    )

    assert response.status_code == 200

    returned_priorities = [
        notification["priority"]
        for notification in response.json()
    ]

    # Verify high-to-low priority ordering
    assert returned_priorities == ["high", "normal", "low"]


def test_idempotency_key_prevents_duplicate_notifications(client):
    headers = {
        "Idempotency-Key": "test-key-001"
    }

    payload = {
        "to": "+919876543210",
        "message": "My OTP notification",
        "priority": "high",
    }

    first_response = client.post(
        "/notifications",
        json=payload,
        headers=headers,
    )

    second_response = client.post(
        "/notifications",
        json=payload,
        headers=headers,
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 202

    assert (
        first_response.json()["id"]
        == second_response.json()["id"]
    )


def test_idempotency_key_with_different_payload_returns_409(client):
    headers = {
        "Idempotency-Key": "test-key-002"
    }

    first_response = client.post(
        "/notifications",
        json={
            "to": "+919876543210",
            "message": "Original OTP",
            "priority": "high",
        },
        headers=headers,
    )

    second_response = client.post(
        "/notifications",
        json={
            "to": "+919876543210",
            "message": "Different message",
            "priority": "high",
        },
        headers=headers,
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 409

    assert second_response.json()["detail"] == (
        "Idempotency key already used "
        "with a different request payload"
    )


def test_different_idempotency_keys_create_separate_notifications(client):
    payload = {
        "to": "+919876543210",
        "message": "New notification",
        "priority": "normal",
    }

    first_response = client.post(
        "/notifications",
        json=payload,
        headers={"Idempotency-Key": "test-key-003"},
    )

    second_response = client.post(
        "/notifications",
        json=payload,
        headers={"Idempotency-Key": "test-key-004"},
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 202

    assert (
        first_response.json()["id"]
        != second_response.json()["id"]
    )

def test_create_notification_without_idempotency_key(client):
    payload = {
        "to": "+919876543210",
        "message": "Notification without idempotency key",
        "priority": "normal",
    }

    first_response = client.post(
        "/notifications",
        json=payload,
    )

    second_response = client.post(
        "/notifications",
        json=payload,
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 202

    assert (
        first_response.json()["id"]
        != second_response.json()["id"]
    )


def test_create_notification_creates_outbox_event(client, db_session):
    response = client.post(
        "/notifications",
        json={
            "to": "+919876543210",
            "message": "Test outbox event creation",
            "priority": "high",
        },
    )

    assert response.status_code == 202

    notification_id = response.json()["id"]

    events = (
        db_session.query(OutboxEvent)
        .filter(OutboxEvent.notification_id == notification_id)
        .all()
    )

    assert len(events) == 1

    event = events[0]

    assert event.event_type == "notification.created"
    assert event.payload["notification_id"] == notification_id
    assert event.payload["recipient"] == "+919876543210"
    assert event.payload["message"] == "Test outbox event creation"
    assert event.payload["priority"] == "high"
    assert event.published_at is None
    assert event.attempts == 0


def test_idempotency_does_not_create_duplicate_outbox_events(
    client,
    db_session,
):
    headers = {
        "Idempotency-Key": "outbox-idempotency-test-001"
    }

    payload = {
        "to": "+919876543210",
        "message": "Test outbox idempotency",
        "priority": "normal",
    }

    first_response = client.post(
        "/notifications",
        json=payload,
        headers=headers,
    )

    second_response = client.post(
        "/notifications",
        json=payload,
        headers=headers,
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 202

    notification_id = first_response.json()["id"]

    assert second_response.json()["id"] == notification_id

    event_count = (
        db_session.query(OutboxEvent)
        .filter(OutboxEvent.notification_id == notification_id)
        .count()
    )

    assert event_count == 1


def test_notification_rolls_back_if_outbox_creation_fails(
    client,
    db_session,
    monkeypatch,
):
    original_add = db_session.add

    def fail_when_adding_outbox(instance, _warn=True):
        if isinstance(instance, OutboxEvent):
            raise RuntimeError("Simulated outbox creation failure")

        return original_add(instance, _warn=_warn)

    monkeypatch.setattr(db_session, "add", fail_when_adding_outbox)

    idempotency_key = "outbox-rollback-test-001"

    with pytest.raises(
        RuntimeError,
        match="Simulated outbox creation failure",
    ):
        client.post(
            "/notifications",
            json={
                "to": "+919876543210",
                "message": "Test transaction rollback",
                "priority": "high",
            },
            headers={
                "Idempotency-Key": idempotency_key,
            },
        )

    # Explicitly roll back the uncommitted transaction.
    db_session.rollback()

    notification_count = (
        db_session.query(Notification)
        .filter(
            Notification.idempotency_key == idempotency_key
        )
        .count()
    )

    assert notification_count == 0


def create_notification_for_dlr(
    db_session,
    provider_message_id,
    initial_status="sent",
):
    """Create a notification for testing delivery receipts."""
    notification = Notification(
        recipient="+919876543210",
        message="Test DLR notification",
        status=initial_status,
        priority="normal",
        provider_name="mock",
        provider_message_id=provider_message_id,
        provider_status="accepted" if initial_status == "sent" else None,
    )

    db_session.add(notification)
    db_session.flush()

    return notification


def test_delivery_receipt_updates_notification_to_delivered(
    client,
    db_session,
):
    notification = create_notification_for_dlr(
        db_session,
        "test-dlr-delivered-001",
    )

    response = post_signed_dlr(
        client,
        {
            "provider_message_id": "test-dlr-delivered-001",
            "status": "delivered",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "delivered"

    db_session.refresh(notification)

    assert notification.status == "delivered"
    assert notification.provider_status == "delivered"
    assert notification.delivered_at is not None
    assert notification.failed_at is None

    saved_receipt = (
        db_session.query(DeliveryReceipt)
        .filter_by(notification_id=notification.id)
        .one()
    )

    assert saved_receipt.status == "delivered"
    assert saved_receipt.provider_message_id == (
        "test-dlr-delivered-001"
    )
    assert saved_receipt.payload["status"] == "delivered"
    assert len(saved_receipt.receipt_fingerprint) == 64


def test_delivery_receipt_updates_notification_to_failed(
    client,
    db_session,
):
    notification = create_notification_for_dlr(
        db_session,
        "test-dlr-failed-001",
    )

    response = post_signed_dlr(
        client,
        {
            "provider_message_id": "test-dlr-failed-001",
            "status": "failed",
            "failure_reason": "Invalid destination number",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "failed"

    db_session.refresh(notification)

    assert notification.status == "failed"
    assert notification.provider_status == "failed"
    assert notification.failed_at is not None
    assert notification.delivered_at is None
    assert notification.failure_reason == "Invalid destination number"

    saved_receipt = (
        db_session.query(DeliveryReceipt)
        .filter_by(notification_id=notification.id)
        .one()
    )

    assert saved_receipt.status == "failed"
    assert saved_receipt.failure_reason == (
        "Invalid destination number"
    )
    assert len(saved_receipt.receipt_fingerprint) == 64


def test_duplicate_delivery_receipt_is_ignored(
    client,
    db_session,
):
    notification = create_notification_for_dlr(
        db_session,
        "test-dlr-duplicate-001",
    )

    payload = {
        "provider_message_id": "test-dlr-duplicate-001",
        "status": "delivered",
    }

    first_response = post_signed_dlr(client, payload)
    assert first_response.status_code == 200

    db_session.refresh(notification)
    original_delivered_at = notification.delivered_at

    # A different signature timestamp must not create a new logical receipt.
    second_response = post_signed_dlr(
        client,
        payload,
        timestamp=int(time.time()) + 1,
    )

    assert second_response.status_code == 200
    assert second_response.json()["message"] == (
        "Duplicate delivery receipt ignored"
    )

    db_session.refresh(notification)

    assert notification.status == "delivered"
    assert notification.delivered_at == original_delivered_at

    receipt_count = (
        db_session.query(DeliveryReceipt)
        .filter_by(notification_id=notification.id)
        .count()
    )

    assert receipt_count == 1


def test_conflicting_delivery_receipt_returns_409(
    client,
    db_session,
):
    notification = create_notification_for_dlr(
        db_session,
        "test-dlr-conflict-001",
    )

    delivered_response = post_signed_dlr(
        client,
        {
            "provider_message_id": "test-dlr-conflict-001",
            "status": "delivered",
        },
    )

    assert delivered_response.status_code == 200

    failed_response = post_signed_dlr(
        client,
        {
            "provider_message_id": "test-dlr-conflict-001",
            "status": "failed",
            "failure_reason": "Simulated failure",
        },
    )

    assert failed_response.status_code == 409

    db_session.refresh(notification)

    assert notification.status == "delivered"
    assert notification.failed_at is None
    assert notification.failure_reason is None

    receipts = (
        db_session.query(DeliveryReceipt)
        .filter_by(notification_id=notification.id)
        .all()
    )

    assert len(receipts) == 2
    assert {receipt.status for receipt in receipts} == {
        "delivered",
        "failed",
    }


def test_delivery_receipt_for_unknown_provider_id_returns_404(client):
    response = post_signed_dlr(
        client,
        {
            "provider_message_id": "unknown-provider-message",
            "status": "delivered",
        },
    )

    assert response.status_code == 404


@pytest.mark.parametrize(
    "initial_status",
    ["queued", "processing"],
)
def test_delivery_receipt_rejected_before_notification_is_sent(
    client,
    db_session,
    initial_status,
):
    notification = create_notification_for_dlr(
        db_session,
        f"test-dlr-not-sent-{initial_status}",
        initial_status=initial_status,
    )

    response = post_signed_dlr(
        client,
        {
            "provider_message_id": notification.provider_message_id,
            "status": "delivered",
        },
    )

    assert response.status_code == 409

    db_session.refresh(notification)

    assert notification.status == initial_status
    assert notification.delivered_at is None

    receipt_count = (
        db_session.query(DeliveryReceipt)
        .filter_by(notification_id=notification.id)
        .count()
    )

    assert receipt_count == 0


def test_dlr_rejects_missing_authentication_headers(client):
    response = client.post(
        "/webhooks/dlr",
        json={
            "provider_message_id": "unsigned-message",
            "status": "delivered",
        },
    )

    assert response.status_code == 401
    assert response.json()["detail"] == (
        "Missing webhook authentication headers"
    )


def test_dlr_rejects_invalid_signature(client, db_session):
    notification = create_notification_for_dlr(
        db_session,
        "test-dlr-invalid-signature",
    )

    response = post_signed_dlr(
        client,
        {
            "provider_message_id": "test-dlr-invalid-signature",
            "status": "delivered",
        },
        signing_secret="incorrect-secret",
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid webhook signature"

    db_session.refresh(notification)
    assert notification.status == "sent"


def test_dlr_rejects_expired_timestamp(client, db_session):
    notification = create_notification_for_dlr(
        db_session,
        "test-dlr-expired-timestamp",
    )

    expired_timestamp = (
        int(time.time())
        - settings.DLR_WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS
        - 1
    )

    response = post_signed_dlr(
        client,
        {
            "provider_message_id": "test-dlr-expired-timestamp",
            "status": "delivered",
        },
        timestamp=expired_timestamp,
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Webhook timestamp expired"

    db_session.refresh(notification)
    assert notification.status == "sent"
