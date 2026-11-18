
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class ProviderSendResult:
    provider_name: str
    provider_message_id: str
    provider_status: str


class ProviderError(Exception):
    """Base exception for SMS provider failures."""


class TransientProviderError(ProviderError):
    """Temporary failure that may succeed on retry."""


class PermanentProviderError(ProviderError):
    """Permanent failure that should not be retried."""


class SMSProvider(ABC):

    @abstractmethod
    async def send(
        self,
        recipient: str,
        message: str,
        idempotency_key: str,
    ) -> ProviderSendResult:
        """Submit an SMS and return the provider response."""
        raise NotImplementedError
