
import asyncio

import aiohttp
import pytest
from pywebpush import WebPushException

from app.providers.web_push_provider import (
    ExpiredPushSubscriptionError,
    PermanentPushError,
    TransientPushError,
    WebPushProvider,
)


class FakeResponse:
    def __init__(self, status):
        self.status = status


def create_provider(tmp_path, sender):
    # The constructor checks that the private key file exists.
    # No real VAPID signing or external request happens in these tests.
    key_file = tmp_path / "test-private-key.pem"
    key_file.write_text("fake-private-key")

    return WebPushProvider(
        private_key_path=str(key_file),
        subject="mailto:test@example.com",
        ttl_seconds=3600,
        timeout_seconds=10,
        sender=sender,
    )


def valid_subscription():
    return {
        "endpoint": "https://push.example.com/send/test",
        "keys": {
            "p256dh": "test-public-key",
            "auth": "test-auth-secret",
        },
    }


def test_successful_push_submission(tmp_path):
    calls = []

    async def fake_sender(**kwargs):
        calls.append(kwargs)
        return FakeResponse(status=201)

    provider = create_provider(tmp_path, fake_sender)

    payload = {
        "title": "NotifyFlow",
        "body": "Your notification has arrived",
        "url": "/notifications",
    }

    result = asyncio.run(
        provider.send(valid_subscription(), payload)
    )

    assert result.provider_name == "webpush"
    assert result.provider_status == "accepted"
    assert result.status_code == 201

    assert len(calls) == 1

    request = calls[0]

    assert request["subscription_info"] == valid_subscription()
    assert request["vapid_claims"] == {
        "sub": "mailto:test@example.com",
    }
    assert request["ttl"] == 3600
    assert request["timeout"] == 10
    assert request["data"] == (
        '{"title":"NotifyFlow",'
        '"body":"Your notification has arrived",'
        '"url":"/notifications"}'
    )


@pytest.mark.parametrize("status_code", [404, 410])
def test_expired_subscription_is_classified(
    tmp_path,
    status_code,
):
    async def fake_sender(**kwargs):
        raise WebPushException(
            "Subscription no longer exists",
            response=FakeResponse(status_code),
        )

    provider = create_provider(tmp_path, fake_sender)

    with pytest.raises(ExpiredPushSubscriptionError):
        asyncio.run(
            provider.send(
                valid_subscription(),
                {"title": "Test"},
            )
        )


@pytest.mark.parametrize(
    "status_code",
    [408, 425, 429, 500, 502, 503],
)
def test_temporary_http_errors_are_retryable(
    tmp_path,
    status_code,
):
    async def fake_sender(**kwargs):
        raise WebPushException(
            "Temporary push-service failure",
            response=FakeResponse(status_code),
        )

    provider = create_provider(tmp_path, fake_sender)

    with pytest.raises(TransientPushError):
        asyncio.run(
            provider.send(
                valid_subscription(),
                {"title": "Test"},
            )
        )


def test_permanent_http_error(tmp_path):
    async def fake_sender(**kwargs):
        raise WebPushException(
            "Invalid request",
            response=FakeResponse(400),
        )

    provider = create_provider(tmp_path, fake_sender)

    with pytest.raises(PermanentPushError):
        asyncio.run(
            provider.send(
                valid_subscription(),
                {"title": "Test"},
            )
        )


def test_async_timeout_is_retryable(tmp_path):
    async def fake_sender(**kwargs):
        raise asyncio.TimeoutError("Request timed out")

    provider = create_provider(tmp_path, fake_sender)

    with pytest.raises(TransientPushError):
        asyncio.run(
            provider.send(
                valid_subscription(),
                {"title": "Test"},
            )
        )


def test_aiohttp_connection_error_is_retryable(tmp_path):
    async def fake_sender(**kwargs):
        raise aiohttp.ClientConnectionError(
            "Connection failed"
        )

    provider = create_provider(tmp_path, fake_sender)

    with pytest.raises(TransientPushError):
        asyncio.run(
            provider.send(
                valid_subscription(),
                {"title": "Test"},
            )
        )


def test_missing_endpoint_is_rejected(tmp_path):
    calls = []

    async def fake_sender(**kwargs):
        calls.append(kwargs)
        return FakeResponse(status=201)

    provider = create_provider(tmp_path, fake_sender)

    subscription = {
        "keys": {
            "p256dh": "test-public-key",
            "auth": "test-auth-secret",
        },
    }

    with pytest.raises(PermanentPushError):
        asyncio.run(
            provider.send(subscription, {"title": "Test"})
        )

    assert calls == []


def test_missing_subscription_keys_are_rejected(tmp_path):
    calls = []

    async def fake_sender(**kwargs):
        calls.append(kwargs)
        return FakeResponse(status=201)

    provider = create_provider(tmp_path, fake_sender)

    subscription = {
        "endpoint": "https://push.example.com/send/test",
    }

    with pytest.raises(PermanentPushError):
        asyncio.run(
            provider.send(subscription, {"title": "Test"})
        )

    assert calls == []


def test_non_json_serializable_payload_is_rejected(tmp_path):
    calls = []

    async def fake_sender(**kwargs):
        calls.append(kwargs)
        return FakeResponse(status=201)

    provider = create_provider(tmp_path, fake_sender)

    with pytest.raises(PermanentPushError):
        asyncio.run(
            provider.send(
                valid_subscription(),
                {"invalid": object()},
            )
        )

    assert calls == []
