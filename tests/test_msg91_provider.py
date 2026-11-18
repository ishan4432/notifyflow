import asyncio

import httpx
import pytest

from app.providers.base import (
    PermanentProviderError,
    TransientProviderError,
)
from app.providers.msg91_provider import MSG91Provider


class FakeResponse:
    def __init__(
        self,
        status_code=200,
        payload=None,
        text="",
        invalid_json=False,
    ):
        self.status_code = status_code
        self.payload = payload or {}
        self.text = text
        self.invalid_json = invalid_json

    def json(self):
        if self.invalid_json:
            raise ValueError("Invalid JSON")

        return self.payload


def install_fake_client(
    monkeypatch,
    response=None,
    exception=None,
):
    calls = []

    class FakeAsyncClient:

        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def post(self, url, headers, json):
            calls.append({
                "url": url,
                "headers": headers,
                "json": json,
            })

            if exception is not None:
                raise exception

            return response

    monkeypatch.setattr(
        "app.providers.msg91_provider.httpx.AsyncClient",
        FakeAsyncClient,
    )

    return calls


def create_provider():
    return MSG91Provider(
        auth_key="test-auth-key",
        template_id="test-template-id",
        message_variable="VAR1",
    )


def test_successful_submission(monkeypatch):
    response = FakeResponse(
        status_code=200,
        payload={
            "type": "success",
            "request_id": "msg91-request-123",
        },
    )

    calls = install_fake_client(
        monkeypatch,
        response=response,
    )

    provider = create_provider()

    result = asyncio.run(
        provider.send(
            recipient="+91 98765-43210",
            message="Your order has shipped",
            idempotency_key="notifyflow-event-123",
        )
    )

    assert result.provider_name == "msg91"
    assert result.provider_message_id == "msg91-request-123"
    assert result.provider_status == "accepted"

    assert len(calls) == 1
    assert calls[0]["url"] == (
        "https://control.msg91.com/api/v5/flow"
    )

    assert calls[0]["headers"]["authkey"] == "test-auth-key"

    assert calls[0]["json"] == {
        "template_id": "test-template-id",
        "realTimeResponse": "1",
        "recipients": [
            {
                "mobiles": "919876543210",
                "VAR1": "Your order has shipped",
                "UUID": "notifyflow-event-123",
            }
        ],
    }


def test_provider_error_in_http_200_response(monkeypatch):
    response = FakeResponse(
        status_code=200,
        payload={
            "type": "error",
            "message": "Invalid template ID",
        },
    )

    install_fake_client(monkeypatch, response=response)

    with pytest.raises(PermanentProviderError):
        asyncio.run(
            create_provider().send(
                "+919876543210",
                "Hello",
                "event-1",
            )
        )


def test_unknown_response_type_is_rejected(monkeypatch):
    response = FakeResponse(
        status_code=200,
        payload={
            "request_id": "some-request-id",
        },
    )

    install_fake_client(monkeypatch, response=response)

    with pytest.raises(
        PermanentProviderError,
        match="unexpected response type",
    ):
        asyncio.run(
            create_provider().send(
                "+919876543210",
                "Hello",
                "event-1",
            )
        )


def test_missing_request_id_is_rejected(monkeypatch):
    response = FakeResponse(
        status_code=200,
        payload={
            "type": "success",
        },
    )

    install_fake_client(monkeypatch, response=response)

    with pytest.raises(
        PermanentProviderError,
        match="request ID",
    ):
        asyncio.run(
            create_provider().send(
                "+919876543210",
                "Hello",
                "event-1",
            )
        )


def test_http_401_is_permanent_error(monkeypatch):
    response = FakeResponse(
        status_code=401,
        text="Unauthorized",
    )

    install_fake_client(monkeypatch, response=response)

    with pytest.raises(PermanentProviderError):
        asyncio.run(
            create_provider().send(
                "+919876543210",
                "Hello",
                "event-1",
            )
        )


@pytest.mark.parametrize("status_code", [408, 425, 429, 500, 502, 503])
def test_temporary_http_errors_are_retryable(
    monkeypatch,
    status_code,
):
    response = FakeResponse(
        status_code=status_code,
        text="Temporary provider failure",
    )

    install_fake_client(monkeypatch, response=response)

    with pytest.raises(TransientProviderError):
        asyncio.run(
            create_provider().send(
                "+919876543210",
                "Hello",
                "event-1",
            )
        )


def test_timeout_is_retryable(monkeypatch):
    install_fake_client(
        monkeypatch,
        exception=httpx.TimeoutException("Request timed out"),
    )

    with pytest.raises(TransientProviderError):
        asyncio.run(
            create_provider().send(
                "+919876543210",
                "Hello",
                "event-1",
            )
        )


def test_network_error_is_retryable(monkeypatch):
    install_fake_client(
        monkeypatch,
        exception=httpx.ConnectError("Connection failed"),
    )

    with pytest.raises(TransientProviderError):
        asyncio.run(
            create_provider().send(
                "+919876543210",
                "Hello",
                "event-1",
            )
        )


def test_invalid_recipient_is_rejected_before_http_call(monkeypatch):
    calls = install_fake_client(
        monkeypatch,
        response=FakeResponse(
            payload={
                "type": "success",
                "request_id": "should-not-be-used",
            }
        ),
    )

    with pytest.raises(PermanentProviderError):
        asyncio.run(
            create_provider().send(
                "12345",
                "Hello",
                "event-1",
            )
        )

    assert calls == []


def test_non_json_success_response_is_rejected(monkeypatch):
    response = FakeResponse(
        status_code=200,
        invalid_json=True,
        text="not-json",
    )

    install_fake_client(monkeypatch, response=response)

    with pytest.raises(
        PermanentProviderError,
        match="non-JSON",
    ):
        asyncio.run(
            create_provider().send(
                "+919876543210",
                "Hello",
                "event-1",
            )
        )
