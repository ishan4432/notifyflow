import base64

from app.models import PushSubscription


def b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


VALID_P256DH = b64url(
    b"\x04" + bytes(range(1, 65))
)

VALID_AUTH = b64url(b"a" * 16)

TEST_ENDPOINT = (
    "https://fcm.googleapis.com/fcm/send/test-endpoint-123"
)


def valid_subscription(
    endpoint=TEST_ENDPOINT,
    p256dh=VALID_P256DH,
    auth=VALID_AUTH,
):
    return {
        "endpoint": endpoint,
        "keys": {
            "p256dh": p256dh,
            "auth": auth,
        },
    }


def test_register_push_subscription(client, db_session):
    response = client.post(
        "/push/subscriptions",
        json=valid_subscription(),
    )

    assert response.status_code == 201

    data = response.json()

    assert data["message"] == (
        "Push subscription registered successfully"
    )
    assert isinstance(data["subscription_id"], int)

    stored = (
        db_session.query(PushSubscription)
        .filter_by(endpoint=TEST_ENDPOINT)
        .one()
    )

    assert stored.p256dh_key == VALID_P256DH
    assert stored.auth_key == VALID_AUTH


def test_duplicate_endpoint_updates_existing_subscription(
    client,
    db_session,
):
    first_response = client.post(
        "/push/subscriptions",
        json=valid_subscription(),
    )

    assert first_response.status_code == 201

    first_id = first_response.json()["subscription_id"]

    updated_p256dh = b64url(
        b"\x04" + bytes(range(65, 129))
    )
    updated_auth = b64url(b"z" * 16)

    second_response = client.post(
        "/push/subscriptions",
        json=valid_subscription(
            p256dh=updated_p256dh,
            auth=updated_auth,
        ),
    )

    assert second_response.status_code == 201

    second_id = second_response.json()["subscription_id"]

    # Same endpoint must reuse the original database record.
    assert first_id == second_id

    subscriptions = (
        db_session.query(PushSubscription)
        .filter_by(endpoint=TEST_ENDPOINT)
        .all()
    )

    assert len(subscriptions) == 1

    stored = subscriptions[0]

    assert stored.p256dh_key == updated_p256dh
    assert stored.auth_key == updated_auth


def test_reject_http_endpoint(client):
    response = client.post(
        "/push/subscriptions",
        json=valid_subscription(
            endpoint="http://example.com/push"
        ),
    )

    assert response.status_code == 422


def test_reject_endpoint_with_credentials(client):
    response = client.post(
        "/push/subscriptions",
        json=valid_subscription(
            endpoint="https://user:password@example.com/push"
        ),
    )

    assert response.status_code == 422


def test_reject_invalid_public_key(client):
    response = client.post(
        "/push/subscriptions",
        json=valid_subscription(
            p256dh="not-a-valid-public-key"
        ),
    )

    assert response.status_code == 422


def test_reject_public_key_with_wrong_prefix(client):
    invalid_key = b64url(
        b"\x03" + bytes(range(1, 65))
    )

    response = client.post(
        "/push/subscriptions",
        json=valid_subscription(
            p256dh=invalid_key
        ),
    )

    assert response.status_code == 422


def test_reject_invalid_auth_secret_length(client):
    response = client.post(
        "/push/subscriptions",
        json=valid_subscription(
            auth=b64url(b"short-secret-12")
        ),
    )

    assert response.status_code == 422


def test_reject_missing_subscription_keys(client):
    response = client.post(
        "/push/subscriptions",
        json={
            "endpoint": TEST_ENDPOINT,
        },
    )

    assert response.status_code == 422
