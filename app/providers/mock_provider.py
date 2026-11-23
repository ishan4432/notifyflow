import logging
import os

from app.providers.base import (
    SMSProvider,
    ProviderSendResult,
    PermanentProviderError,
    TransientProviderError,
)

logger = logging.getLogger(__name__)


class MockSMSProvider(SMSProvider):

    async def send(
        self,
        recipient: str,
        message: str,
        idempotency_key: str,
        attempt_number: int = 1,
    ) -> ProviderSendResult:

        if not recipient or not message or not idempotency_key:
            raise PermanentProviderError(
                "Recipient, message and idempotency key are required"
            )

        if (
            not isinstance(attempt_number, int)
            or isinstance(attempt_number, bool)
            or attempt_number < 1
        ):
            raise ValueError("attempt_number must be a positive integer")

        try:
            failures_before_success = int(
                os.getenv(
                    "NOTIFYFLOW_MOCK_FAILURES_BEFORE_SUCCESS",
                    "0",
                )
            )
        except ValueError as exc:
            raise ValueError(
                "NOTIFYFLOW_MOCK_FAILURES_BEFORE_SUCCESS must be an integer"
            ) from exc

        if failures_before_success < 0:
            raise ValueError(
                "NOTIFYFLOW_MOCK_FAILURES_BEFORE_SUCCESS cannot be negative"
            )

        # Controlled retry simulation.
        # The consumer supplies the persistent database attempt count.
        if attempt_number <= failures_before_success:
            logger.warning(
                "MOCK SMS transient failure: notification_key=%s "
                "attempt=%s configured_failures=%s",
                idempotency_key,
                attempt_number,
                failures_before_success,
            )

            raise TransientProviderError(
                f"Simulated transient failure on attempt {attempt_number}"
            )

        # Simulate successful SMS provider submission.
        # No real SMS is sent and no charges are incurred.
        provider_message_id = f"mock-{idempotency_key}"

        logger.info(
            "MOCK SMS accepted: recipient=%s attempt=%s message_id=%s",
            recipient,
            attempt_number,
            provider_message_id,
        )

        return ProviderSendResult(
            provider_name="mock",
            provider_message_id=provider_message_id,
            provider_status="accepted",
        )
