from app.database import settings

from app.providers.base import SMSProvider
from app.providers.mock_provider import MockSMSProvider
from app.providers.msg91_provider import MSG91Provider


def get_sms_provider() -> SMSProvider:

    provider_name = settings.SMS_PROVIDER.strip().lower()

    if provider_name == "mock":
        return MockSMSProvider()

    if provider_name == "msg91":

        auth_key = (
            settings.MSG91_AUTH_KEY.get_secret_value()
            if settings.MSG91_AUTH_KEY
            else ""
        )

        if not auth_key.strip():
            raise ValueError(
                "MSG91_AUTH_KEY is required when SMS_PROVIDER=msg91"
            )

        if not settings.MSG91_TEMPLATE_ID:
            raise ValueError(
                "MSG91_TEMPLATE_ID is required when SMS_PROVIDER=msg91"
            )

        return MSG91Provider(
            auth_key=auth_key,
            template_id=settings.MSG91_TEMPLATE_ID,
            message_variable=settings.MSG91_MESSAGE_VARIABLE,
            api_url=settings.MSG91_API_URL,
            timeout_seconds=settings.MSG91_TIMEOUT_SECONDS,
        )

    raise ValueError(
        f"Unsupported SMS provider: {provider_name}"
    )
