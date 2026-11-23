import logging
import re
from typing import Any

import httpx

from app.providers.base import (
    SMSProvider,
    ProviderSendResult,
    TransientProviderError,
    PermanentProviderError,
)


logger = logging.getLogger(__name__)


class MSG91Provider(SMSProvider):

    def __init__(
        self,
        auth_key: str,
        template_id: str,
        message_variable: str = "VAR1",
        api_url: str = "https://control.msg91.com/api/v5/flow",
        timeout_seconds: float = 10.0,
    ):
        if not auth_key or not auth_key.strip():
            raise ValueError("MSG91 auth key is required")

        if not template_id or not template_id.strip():
            raise ValueError("MSG91 template ID is required")

        if not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*",
            message_variable,
        ):
            raise ValueError("Invalid MSG91 template variable name")

        if message_variable.lower() in {"mobiles", "uuid"}:
            raise ValueError(
                "MSG91 message variable cannot be mobiles or UUID"
            )

        if timeout_seconds <= 0:
            raise ValueError("Timeout must be greater than zero")

        self.auth_key = auth_key
        self.template_id = template_id
        self.message_variable = message_variable
        self.api_url = api_url
        self.timeout_seconds = timeout_seconds

    @staticmethod
    def _normalize_recipient(recipient: str) -> str:
        if not isinstance(recipient, str):
            raise PermanentProviderError(
                "Recipient must be a string"
            )

        normalized = re.sub(
            r"[\s()-]",
            "",
            recipient.strip(),
        )

        if normalized.startswith("+"):
            normalized = normalized[1:]

        if not re.fullmatch(r"[1-9]\d{7,14}", normalized):
            raise PermanentProviderError(
                "Recipient must be an international mobile number "
                "with country code, such as +919876543210"
            )

        return normalized

    @staticmethod
    def _extract_error_message(payload: dict[str, Any]) -> str:
        detail = (
            payload.get("message")
            or payload.get("error")
            or payload.get("errors")
            or "MSG91 rejected the SMS request"
        )

        if isinstance(detail, (dict, list)):
            detail = str(detail)

        return str(detail)[:300]

    @staticmethod
    def _is_provider_error(
        response_data: dict[str, Any],
    ) -> bool:
        response_type = str(
            response_data.get("type", "")
        ).lower()

        response_status = str(
            response_data.get("status", "")
        ).lower()

        return (
            response_type == "error"
            or response_status in {
                "error",
                "failed",
                "failure",
            }
            or response_data.get("hasError") is True
            or bool(response_data.get("errors"))
        )

    @staticmethod
    def _is_provider_success(
        response_data: dict[str, Any],
    ) -> bool:
        response_type = str(
            response_data.get("type", "")
        ).lower()

        response_status = str(
            response_data.get("status", "")
        ).lower()

        return (
            response_type == "success"
            or response_status == "success"
        )

    async def send(
        self,
        recipient: str,
        message: str,
        idempotency_key: str,
        attempt_number: int = 1,
    ) -> ProviderSendResult:

        if (
            not isinstance(message, str)
            or not message.strip()
            or not isinstance(idempotency_key, str)
            or not idempotency_key.strip()
        ):
            raise PermanentProviderError(
                "Recipient, message and idempotency key are required"
            )

        normalized_recipient = self._normalize_recipient(
            recipient
        )

        recipient_data = {
            "mobiles": normalized_recipient,
            self.message_variable: message,
            "UUID": idempotency_key,
        }

        payload = {
            "template_id": self.template_id,
            "realTimeResponse": "1",
            "recipients": [recipient_data],
        }

        headers = {
            "accept": "application/json",
            "authkey": self.auth_key,
            "content-type": "application/json",
        }

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout_seconds
            ) as client:
                response = await client.post(
                    self.api_url,
                    headers=headers,
                    json=payload,
                )

        except httpx.TimeoutException as exc:
            raise TransientProviderError(
                "MSG91 request timed out"
            ) from exc

        except httpx.TransportError as exc:
            raise TransientProviderError(
                "Unable to connect to MSG91"
            ) from exc

        if (
            response.status_code in {408, 425, 429}
            or response.status_code >= 500
        ):
            raise TransientProviderError(
                f"MSG91 temporary HTTP failure: "
                f"{response.status_code}"
            )

        if response.status_code >= 400:
            raise PermanentProviderError(
                f"MSG91 rejected the request with HTTP "
                f"{response.status_code}: "
                f"{response.text[:300]}"
            )

        try:
            response_data = response.json()

        except ValueError as exc:
            raise PermanentProviderError(
                "MSG91 returned a non-JSON response"
            ) from exc

        if not isinstance(response_data, dict):
            raise PermanentProviderError(
                "MSG91 returned an unexpected response format"
            )

        if self._is_provider_error(response_data):
            error_message = self._extract_error_message(
                response_data
            )

            raise PermanentProviderError(
                f"MSG91 API error: {error_message}"
            )

        if not self._is_provider_success(response_data):
            raise PermanentProviderError(
                "MSG91 returned an unexpected response type"
            )

        request_id = (
            response_data.get("request_id")
            or response_data.get("requestId")
        )

        if not request_id:
            raise PermanentProviderError(
                "MSG91 success response did not contain "
                "a request ID"
            )

        provider_message_id = str(request_id)

        logger.info(
            "MSG91 accepted SMS submission: request_id=%s",
            provider_message_id,
        )

        return ProviderSendResult(
            provider_name="msg91",
            provider_message_id=provider_message_id,
            provider_status="accepted",
        )
