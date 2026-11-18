from app.database import settings
from app.providers.web_push_provider import WebPushProvider


def get_web_push_provider() -> WebPushProvider:

    if not settings.VAPID_PRIVATE_KEY_PATH:
        raise ValueError(
            "VAPID_PRIVATE_KEY_PATH is not configured"
        )

    if not settings.VAPID_PUBLIC_KEY:
        raise ValueError(
            "VAPID_PUBLIC_KEY is not configured"
        )

    if not settings.VAPID_SUBJECT:
        raise ValueError(
            "VAPID_SUBJECT is not configured"
        )

    return WebPushProvider(
        private_key_path=settings.VAPID_PRIVATE_KEY_PATH,
        subject=settings.VAPID_SUBJECT,
        ttl_seconds=settings.WEB_PUSH_TTL_SECONDS,
        timeout_seconds=settings.WEB_PUSH_TIMEOUT_SECONDS,
    )
