import pytest
from pydantic import SecretStr

from app.database import settings
from app.providers.factory import get_sms_provider
from app.providers.mock_provider import MockSMSProvider
from app.providers.msg91_provider import MSG91Provider


def test_factory_returns_mock_provider(monkeypatch):
    monkeypatch.setattr(settings, "SMS_PROVIDER", "mock")

    provider = get_sms_provider()

    assert isinstance(provider, MockSMSProvider)


def test_factory_returns_msg91_provider(monkeypatch):
    monkeypatch.setattr(settings, "SMS_PROVIDER", "msg91")
    monkeypatch.setattr(
        settings,
        "MSG91_AUTH_KEY",
        SecretStr("test-auth-key"),
    )
    monkeypatch.setattr(
        settings,
        "MSG91_TEMPLATE_ID",
        "test-template-id",
    )

    provider = get_sms_provider()

    assert isinstance(provider, MSG91Provider)
    assert provider.template_id == "test-template-id"
    assert provider.message_variable == "VAR1"


def test_factory_rejects_missing_msg91_auth_key(monkeypatch):
    monkeypatch.setattr(settings, "SMS_PROVIDER", "msg91")
    monkeypatch.setattr(settings, "MSG91_AUTH_KEY", None)
    monkeypatch.setattr(
        settings,
        "MSG91_TEMPLATE_ID",
        "test-template-id",
    )

    with pytest.raises(ValueError, match="MSG91_AUTH_KEY"):
        get_sms_provider()


def test_factory_rejects_missing_msg91_template_id(monkeypatch):
    monkeypatch.setattr(settings, "SMS_PROVIDER", "msg91")
    monkeypatch.setattr(
        settings,
        "MSG91_AUTH_KEY",
        SecretStr("test-auth-key"),
    )
    monkeypatch.setattr(settings, "MSG91_TEMPLATE_ID", None)

    with pytest.raises(ValueError, match="MSG91_TEMPLATE_ID"):
        get_sms_provider()


def test_factory_rejects_unknown_provider(monkeypatch):
    monkeypatch.setattr(settings, "SMS_PROVIDER", "unknown")

    with pytest.raises(ValueError, match="Unsupported SMS provider"):
        get_sms_provider()
