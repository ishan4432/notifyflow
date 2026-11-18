from typing import Literal

from pydantic import BaseModel, Field


class NotificationCreate(BaseModel):
    to: str = Field(
        ...,
        min_length=8,
        max_length=16,
        description="Recipient phone number with country code",
    )

    message: str = Field(
        ...,
        min_length=1,
        max_length=1600,
        description="Notification message content",
    )

    priority: Literal["low", "normal", "high"] = Field(
        default="normal",
        description="Notification delivery priority",
    )

class DeliveryReceiptRequest(BaseModel):
    provider_message_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Message ID returned by the SMS provider",
    )

    status: Literal["delivered", "failed"] = Field(
        ...,
        description="Final delivery status reported by the provider",
    )

    failure_reason: str | None = Field(
        default=None,
        max_length=1000,
        description="Reason for delivery failure, if applicable",
    )
