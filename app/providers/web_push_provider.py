
import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

import aiohttp
from pywebpush import WebPushException, webpush_async


logger = logging.getLogger(__name__)


class WebPushProviderError(Exception):
    """Base error for Web Push delivery."""


class ExpiredPushSubscriptionError(WebPushProviderError):
    """Subscription is no longer valid and should be removed."""


class TransientPushError(WebPushProviderError):
    """Temporary push-service failure; retry may succeed."""


class PermanentPushError(WebPushProviderError):
    """Permanent configuration, payload or provider rejection."""


@dataclass(frozen=True)
class WebPushSendResult:
    provider_name: str
    provider_status: str
    status_code: int


AsyncWebPushSender = Callable[..., Awaitable[Any]]


class WebPushProvider:

    def __init__(
        self,
        private_key_path: str,
        subject: str,
        ttl_seconds: int = 3600,
        timeout_seconds: float = 10.0,
        sender: AsyncWebPushSender | None = None,
    ):
        key_path = Path(private_key_path).expanduser()

        if not key_path.is_file():
            raise ValueError(
                "VAPID private key file does not exist"
            )

        if not subject or not (
            subject.startswith("mailto:")
            or subject.startswith("https://")
        ):
            raise ValueError(
                "VAPID subject must be a mailto: or HTTPS URL"
            )

        if ttl_seconds <= 0:
            raise ValueError(
                "Web Push TTL must be greater than zero"
            )

        if timeout_seconds <= 0:
            raise ValueError(
                "Web Push timeout must be greater than zero"
            )

        self.private_key_path = str(key_path)
        self.subject = subject
        self.ttl_seconds = ttl_seconds
        self.timeout_seconds = timeout_seconds

        # Injectable sender makes provider tests independent
        # of external push services.
        self.sender = sender or webpush_async

    @staticmethod
    def validate_subscription(
        subscription_info: dict[str, Any],
    ) -> None:

        if not isinstance(subscription_info, dict):
            raise PermanentPushError(
                "Subscription must be a dictionary"
            )

        endpoint = subscription_info.get("endpoint")
        keys = subscription_info.get("keys")

        if not isinstance(endpoint, str) or not endpoint:
            raise PermanentPushError(
                "Subscription endpoint is missing"
            )

        if not isinstance(keys, dict):
            raise PermanentPushError(
                "Subscription keys are missing"
            )

        for key_name in ("p256dh", "auth"):
            value = keys.get(key_name)

            if not isinstance(value, str) or not value:
                raise PermanentPushError(
                    f"Subscription key is missing: {key_name}"
                )

    async def send(
        self,
        subscription_info: dict[str, Any],
        payload: dict[str, Any],
    ) -> WebPushSendResult:

        self.validate_subscription(subscription_info)

        if not isinstance(payload, dict):
            raise PermanentPushError(
                "Push notification payload must be a dictionary"
            )

        try:
            serialized_payload = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            )

        except (TypeError, ValueError) as exc:
            raise PermanentPushError(
                "Push notification payload is not JSON serializable"
            ) from exc

        try:
            response = await self.sender(
                subscription_info=subscription_info,
                data=serialized_payload,
                vapid_private_key=self.private_key_path,
                vapid_claims={
                    "sub": self.subject,
                },
                ttl=self.ttl_seconds,
                timeout=self.timeout_seconds,
            )

        except WebPushException as exc:
            status_code = exc.status_code

            if status_code in {404, 410}:
                raise ExpiredPushSubscriptionError(
                    f"Push subscription expired: HTTP {status_code}"
                ) from exc

            if (
                status_code in {408, 425, 429}
                or (
                    status_code is not None
                    and status_code >= 500
                )
            ):
                raise TransientPushError(
                    f"Temporary push-service failure: HTTP {status_code}"
                ) from exc

            raise PermanentPushError(
                f"Push service rejected notification: HTTP {status_code}"
            ) from exc

        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            raise TransientPushError(
                f"Push-service connection failed: {type(exc).__name__}"
            ) from exc

        except (ValueError, TypeError) as exc:
            raise PermanentPushError(
                "Invalid Web Push payload or VAPID configuration"
            ) from exc

        status_code = getattr(response, "status", None)

        if not isinstance(status_code, int):
            raise PermanentPushError(
                "Push service returned an unexpected response"
            )

        logger.info(
            "Web Push accepted by push service: HTTP %s",
            status_code,
        )

        return WebPushSendResult(
            provider_name="webpush",
            provider_status="accepted",
            status_code=status_code,
        )
