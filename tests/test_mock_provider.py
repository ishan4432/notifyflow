import asyncio

import pytest

from app.providers.base import TransientProviderError
from app.providers.mock_provider import MockSMSProvider


def test_mock_provider_succeeds_by_default(monkeypatch):
    monkeypatch.delenv(
        "NOTIFYFLOW_MOCK_FAILURES_BEFORE_SUCCESS",
        raising=False,
    )

    provider = MockSMSProvider()

    result = asyncio.run(
        provider.send(
            recipient="+919876543210",
            message="Test notification",
            idempotency_key="notifyflow-notification-1001",
            attempt_number=1,
        )
    )

    assert result.provider_name == "mock"
    assert result.provider_message_id == (
        "mock-notifyflow-notification-1001"
    )
    assert result.provider_status == "accepted"


def test_mock_provider_fails_twice_then_succeeds(monkeypatch):
    monkeypatch.setenv(
        "NOTIFYFLOW_MOCK_FAILURES_BEFORE_SUCCESS",
        "2",
    )

    provider = MockSMSProvider()

    for attempt in (1, 2):
        with pytest.raises(TransientProviderError):
            asyncio.run(
                provider.send(
                    recipient="+919876543210",
                    message="Retry test",
                    idempotency_key="notifyflow-notification-1002",
                    attempt_number=attempt,
                )
            )

    result = asyncio.run(
        provider.send(
            recipient="+919876543210",
            message="Retry test",
            idempotency_key="notifyflow-notification-1002",
            attempt_number=3,
        )
    )

    assert result.provider_status == "accepted"


def test_mock_provider_fails_on_all_three_attempts(monkeypatch):
    monkeypatch.setenv(
        "NOTIFYFLOW_MOCK_FAILURES_BEFORE_SUCCESS",
        "3",
    )

    provider = MockSMSProvider()

    for attempt in (1, 2, 3):
        with pytest.raises(TransientProviderError):
            asyncio.run(
                provider.send(
                    recipient="+919876543210",
                    message="Retry exhaustion test",
                    idempotency_key="notifyflow-notification-1003",
                    attempt_number=attempt,
                )
            )


def test_mock_provider_rejects_invalid_attempt_number():
    provider = MockSMSProvider()

    with pytest.raises(ValueError):
        asyncio.run(
            provider.send(
                recipient="+919876543210",
                message="Invalid attempt test",
                idempotency_key="notifyflow-notification-1004",
                attempt_number=0,
            )
        )
