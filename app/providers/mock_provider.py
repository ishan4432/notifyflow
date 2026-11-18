
import logging

from app.providers.base import (
    SMSProvider,
    ProviderSendResult,
    PermanentProviderError,
)


logger = logging.getLogger(__name__)


class MockSMSProvider(SMSProvider):

    async def send(
        self,
        recipient: str,
        message: str,
        idempotency_key: str,
    ) -> ProviderSendResult:

        if not recipient or not message or not idempotency_key:
            raise PermanentProviderError(
                "Recipient, message and idempotency key are required"
            )

        # Simulate successful SMS provider submission.
        # No real SMS is sent and no charges are incurred.
        provider_message_id = f"mock-{idempotency_key}"

        logger.info(
            "MOCK SMS accepted: recipient=%s, message_id=%s",
            recipient,
            provider_message_id,
        )

        return ProviderSendResult(
            provider_name="mock",
            provider_message_id=provider_message_id,
            provider_status="accepted",
        )
