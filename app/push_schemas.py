import base64
import re
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, field_validator


def decode_base64url(value: str) -> bytes:
    padded = value + "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(padded)


class PushSubscriptionKeys(BaseModel):
    p256dh: str = Field(
        min_length=1,
        max_length=255,
    )

    auth: str = Field(
        min_length=1,
        max_length=255,
    )

    @field_validator("p256dh")
    @classmethod
    def validate_public_key(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", value):
            raise ValueError("Invalid p256dh base64url encoding")

        try:
            decoded = decode_base64url(value)
        except Exception as exc:
            raise ValueError("Invalid p256dh public key") from exc

        # Uncompressed P-256 public key: 65 bytes, beginning with 0x04.
        if len(decoded) != 65 or decoded[0] != 4:
            raise ValueError("Invalid P-256 public key")

        return value

    @field_validator("auth")
    @classmethod
    def validate_auth_secret(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", value):
            raise ValueError("Invalid auth base64url encoding")

        try:
            decoded = decode_base64url(value)
        except Exception as exc:
            raise ValueError("Invalid authentication secret") from exc

        if len(decoded) != 16:
            raise ValueError("Push authentication secret must be 16 bytes")

        return value


class PushSubscriptionCreate(BaseModel):
    endpoint: str = Field(
        min_length=1,
        max_length=4096,
    )

    keys: PushSubscriptionKeys

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)

        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
        ):
            raise ValueError(
                "Push endpoint must be a valid HTTPS URL"
            )

        return value

class PushNotificationCreate(BaseModel):
    title: str = Field(
        min_length=1,
        max_length=120,
    )

    body: str = Field(
        min_length=1,
        max_length=2000,
    )

    target_url: str = Field(
        default="/",
        max_length=2048,
    )

    @field_validator("title", "body")
    @classmethod
    def validate_text(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise ValueError("Value cannot be empty")

        return value

    @field_validator("target_url")
    @classmethod
    def validate_target_url(cls, value: str) -> str:
        value = value.strip()
        parsed = urlsplit(value)

        # Notification clicks must stay within the dashboard origin.
        if (
            not value.startswith("/")
            or value.startswith("//")
            or "\\" in value
            or parsed.scheme
            or parsed.netloc
        ):
            raise ValueError(
                "Target URL must be a same-origin relative path"
            )

        return value
