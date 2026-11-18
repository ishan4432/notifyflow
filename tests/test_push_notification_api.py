
from app.models import PushNotification, PushOutboxEvent


def notification_payload(**overrides):
    payload = {
        "title": "NotifyFlow Test",
        "body": "Your order has been updated.",
        "target_url": "/notifications",
    }
    payload.update(overrides)
    return payload


def test_create_push_notification_creates_outbox(
    client,
    db_session,
):
    response = client.post(
        "/push/notifications",
        json=notification_payload(),
    )

    assert response.status_code == 202

    data = response.json()

    assert isinstance(data["id"], int)
    assert data["title"] == "NotifyFlow Test"
    assert data["status"] == "queued"
    assert data["duplicate"] is False

    notification = db_session.get(
        PushNotification,
        data["id"],
    )

    assert notification is not None
    assert notification.status == "queued"

    events = (
        db_session.query(PushOutboxEvent)
        .filter_by(push_notification_id=data["id"])
        .all()
    )

    assert len(events) == 1
    assert events[0].event_type == "push.notification.created"
    assert events[0].payload == {
        "push_notification_id": data["id"],
    }


def test_same_idempotency_key_returns_existing_notification(
    client,
    db_session,
):
    headers = {"Idempotency-Key": "push-idempotency-001"}
    payload = notification_payload()

    first = client.post(
        "/push/notifications",
        json=payload,
        headers=headers,
    )

    second = client.post(
        "/push/notifications",
        json=payload,
        headers=headers,
    )

    assert first.status_code == 202
    assert second.status_code == 202

    assert first.json()["id"] == second.json()["id"]
    assert first.json()["duplicate"] is False
    assert second.json()["duplicate"] is True

    assert db_session.query(PushNotification).count() == 1
    assert db_session.query(PushOutboxEvent).count() == 1


def test_idempotency_key_with_different_payload_returns_409(
    client,
    db_session,
):
    headers = {"Idempotency-Key": "push-idempotency-002"}

    first = client.post(
        "/push/notifications",
        json=notification_payload(),
        headers=headers,
    )

    assert first.status_code == 202

    second = client.post(
        "/push/notifications",
        json=notification_payload(
            body="A different notification"
        ),
        headers=headers,
    )

    assert second.status_code == 409

    assert db_session.query(PushNotification).count() == 1
    assert db_session.query(PushOutboxEvent).count() == 1


def test_requests_without_idempotency_keys_create_separate_notifications(
    client,
    db_session,
):
    first = client.post(
        "/push/notifications",
        json=notification_payload(),
    )

    second = client.post(
        "/push/notifications",
        json=notification_payload(),
    )

    assert first.status_code == 202
    assert second.status_code == 202

    assert first.json()["id"] != second.json()["id"]

    assert db_session.query(PushNotification).count() == 2
    assert db_session.query(PushOutboxEvent).count() == 2


def test_external_target_url_is_rejected(client):
    response = client.post(
        "/push/notifications",
        json=notification_payload(
            target_url="https://example.com/phishing"
        ),
    )

    assert response.status_code == 422


def test_protocol_relative_target_url_is_rejected(client):
    response = client.post(
        "/push/notifications",
        json=notification_payload(
            target_url="//example.com/phishing"
        ),
    )

    assert response.status_code == 422


def test_blank_title_is_rejected(client):
    response = client.post(
        "/push/notifications",
        json=notification_payload(title="   "),
    )

    assert response.status_code == 422


def test_blank_body_is_rejected(client):
    response = client.post(
        "/push/notifications",
        json=notification_payload(body="   "),
    )

    assert response.status_code == 422
