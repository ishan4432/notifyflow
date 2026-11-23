import asyncio
import json
import logging
import os

from datetime import datetime, timezone
from time import perf_counter

import aio_pika
from prometheus_client import start_http_server

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import SessionLocal, settings
from app.models import ConsumerInbox, Notification
from app.providers.base import (
    PermanentProviderError,
    TransientProviderError,
)
from app.providers.factory import get_sms_provider
from app.worker_metrics import (
    SMS_PROVIDER_ATTEMPTS,
    SMS_DELIVERY_RESULTS,
    SMS_DLQ_EVENTS,
    SMS_MESSAGE_PROCESSING_DURATION,
    SMS_CONSUMER_ERRORS,
)


MAIN_QUEUE = "notifyflow.notifications"
RETRY_QUEUE = "notifyflow.retry"
DLQ_QUEUE = "notifyflow.dlq"

MAX_ATTEMPTS = 3
RETRY_COUNT_HEADER = "x-retry-count"
PUBLISH_TIMEOUT = 10

TERMINAL_STATUSES = {"sent", "delivered", "failed"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("notifyflow.consumer")


class InvalidMessage(Exception):
    """Raised when a RabbitMQ message is permanently invalid."""


def get_retry_count(message):
    headers = message.headers or {}
    value = headers.get(RETRY_COUNT_HEADER, 0)

    try:
        count = int(value)
    except (TypeError, ValueError):
        raise InvalidMessage("Invalid retry count")

    if count < 0:
        raise InvalidMessage("Retry count cannot be negative")

    return count


def insert_inbox_record(db, event_id, notification_id):
    statement = (
        pg_insert(ConsumerInbox)
        .values(
            event_id=event_id,
            notification_id=notification_id,
        )
        .on_conflict_do_nothing(
            index_elements=[ConsumerInbox.event_id]
        )
    )

    db.execute(statement)


async def publish_copy(
    message,
    publisher_channel,
    destination,
    extra_headers,
):
    headers = dict(message.headers or {})

    # RabbitMQ should generate fresh dead-letter history.
    for key in (
        "x-death",
        "x-first-death-queue",
        "x-first-death-reason",
        "x-first-death-exchange",
        "x-last-death-queue",
        "x-last-death-reason",
        "x-last-death-exchange",
    ):
        headers.pop(key, None)

    headers.update(extra_headers)

    outgoing = aio_pika.Message(
        body=message.body,
        headers=headers,
        content_type=message.content_type or "application/json",
        content_encoding=message.content_encoding,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        priority=message.priority,
        correlation_id=message.correlation_id,
        message_id=message.message_id,
    )

    await publisher_channel.default_exchange.publish(
        outgoing,
        routing_key=destination,
        mandatory=True,
        timeout=PUBLISH_TIMEOUT,
    )


async def forward_and_ack(
    message,
    publisher_channel,
    destination,
    extra_headers,
):
    try:
        await publish_copy(
            message,
            publisher_channel,
            destination,
            extra_headers,
        )

    except Exception:
        logger.exception(
            "Failed to publish event %s to %s",
            message.message_id,
            destination,
        )

        try:
            await message.nack(requeue=True)
        except Exception:
            logger.exception("Could not requeue original message")

        return False

    # ACK original only after confirmed publication of its copy.
    await message.ack()
    return True


async def send_to_dlq(message, publisher_channel, reason):
    forwarded = await forward_and_ack(
        message,
        publisher_channel,
        DLQ_QUEUE,
        {
            "x-failure-reason": reason[:1000],
        },
    )

    if forwarded:
        SMS_DLQ_EVENTS.inc()

        logger.error(
            "Event %s moved to DLQ: %s",
            message.message_id,
            reason,
        )


def mark_failed_after_retries(notification_id, reason):
    """
    Best-effort database reconciliation when retry attempts
    have been exhausted.

    If PostgreSQL itself is unavailable, the message still
    goes to the DLQ and this update can be reconciled later.
    """

    if notification_id is None:
        return

    marked_failed = False

    try:
        with SessionLocal.begin() as db:
            notification = db.execute(
                select(Notification)
                .where(Notification.id == notification_id)
                .with_for_update()
            ).scalar_one_or_none()

            if notification is None:
                return

            if notification.status not in TERMINAL_STATUSES:
                notification.status = "failed"
                notification.provider_status = "retry_exhausted"
                notification.failed_at = datetime.now(timezone.utc)
                notification.failure_reason = reason[:2000]
                marked_failed = True

        # Count only after the database transaction commits.
        if marked_failed:
            SMS_DELIVERY_RESULTS.labels(status="failed").inc()

    except Exception:
        logger.exception(
            "Could not update notification %s after retry exhaustion",
            notification_id,
        )


async def schedule_retry(
    message,
    publisher_channel,
    retry_count,
    reason,
    notification_id=None,
):
    attempt_number = retry_count + 1

    if attempt_number >= MAX_ATTEMPTS:

        mark_failed_after_retries(
            notification_id,
            reason,
        )

        await send_to_dlq(
            message,
            publisher_channel,
            (
                f"Maximum processing attempts reached: "
                f"{MAX_ATTEMPTS}. Last error: {reason}"
            ),
        )
        return

    next_retry_count = retry_count + 1

    forwarded = await forward_and_ack(
        message,
        publisher_channel,
        RETRY_QUEUE,
        {
            RETRY_COUNT_HEADER: next_retry_count,
        },
    )

    if forwarded:
        SMS_DELIVERY_RESULTS.labels(status="retrying").inc()

        logger.warning(
            "Event %s failed on attempt %s. "
            "Scheduled retry %s.",
            message.message_id,
            attempt_number,
            next_retry_count,
        )


async def process_message(
    message,
    publisher_channel,
    provider,
):
    event_id = message.message_id
    notification_id = None

    # 1. Validate incoming event.
    try:
        if not event_id:
            raise InvalidMessage("Missing message_id")

        event_id = str(event_id)
        retry_count = get_retry_count(message)

        payload = json.loads(message.body.decode("utf-8"))

        if not isinstance(payload, dict):
            raise InvalidMessage("Payload must be a JSON object")

        notification_id = payload.get("notification_id")

        if type(notification_id) is not int or notification_id <= 0:
            raise InvalidMessage("Invalid notification_id")

        for field in ("recipient", "message", "priority"):
            if not isinstance(payload.get(field), str):
                raise InvalidMessage(
                    f"Missing or invalid field: {field}"
                )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        InvalidMessage,
    ) as exc:
        await send_to_dlq(
            message,
            publisher_channel,
            str(exc),
        )
        return

    skip_provider = False

    # 2. Load and lock the notification.
    # Do not insert the inbox record before provider submission.
    try:
        with SessionLocal.begin() as db:

            notification = db.execute(
                select(Notification)
                .where(Notification.id == notification_id)
                .with_for_update()
            ).scalar_one_or_none()

            if notification is None:
                raise InvalidMessage(
                    f"Notification {notification_id} not found"
                )

            # Ensure the event matches the actual database record.
            if (
                notification.recipient != payload["recipient"]
                or notification.message != payload["message"]
                or notification.priority != payload["priority"]
            ):
                raise InvalidMessage(
                    "Event payload does not match notification record"
                )

            existing_inbox = db.get(
                ConsumerInbox,
                event_id,
            )

            if (
                existing_inbox is not None
                and existing_inbox.notification_id != notification_id
            ):
                raise InvalidMessage(
                    "Event ID belongs to a different notification"
                )

            # A terminal notification must not be submitted again.
            if notification.status in TERMINAL_STATUSES:

                if existing_inbox is None:
                    insert_inbox_record(
                        db,
                        event_id,
                        notification_id,
                    )

                skip_provider = True

            else:
                # Allows previously interrupted processing to resume.
                notification.status = "processing"
                notification.provider_attempts += 1
                notification.provider_name = settings.SMS_PROVIDER
                # Capture the committed, persistent attempt number for the provider.
                attempt_number = notification.provider_attempts

        # Transaction commits here.

    except InvalidMessage as exc:
        await send_to_dlq(
            message,
            publisher_channel,
            str(exc),
        )
        return

    except Exception as exc:
        logger.exception(
            "Database preparation failed for event %s",
            event_id,
        )

        await schedule_retry(
            message,
            publisher_channel,
            retry_count,
            str(exc),
            notification_id,
        )
        return

    if skip_provider:
        await message.ack()

        logger.info(
            "Notification %s is already in terminal state. "
            "Skipping provider submission.",
            notification_id,
        )
        return

    # 3. Submit SMS through the selected provider.
    # The same key is reused across retries.
    provider_idempotency_key = (
        f"notifyflow-notification-{notification_id}"
    )

    try:
        # Count an actual provider call, not just entry into processing.
        SMS_PROVIDER_ATTEMPTS.inc()

        result = await provider.send(
            recipient=payload["recipient"],
            message=payload["message"],
            idempotency_key=provider_idempotency_key,
            attempt_number=attempt_number,
        )

    except PermanentProviderError as exc:

        # Permanent provider rejection: record terminal failure.
        try:
            with SessionLocal.begin() as db:

                notification = db.execute(
                    select(Notification)
                    .where(Notification.id == notification_id)
                    .with_for_update()
                ).scalar_one()

                notification.status = "failed"
                notification.provider_name = settings.SMS_PROVIDER
                notification.provider_status = "rejected"
                notification.failed_at = datetime.now(timezone.utc)
                notification.failure_reason = str(exc)[:2000]

                insert_inbox_record(
                    db,
                    event_id,
                    notification_id,
                )

            # Permanent failure has been committed to PostgreSQL.
            SMS_DELIVERY_RESULTS.labels(status="failed").inc()

            await message.ack()

            logger.error(
                "SMS permanently rejected for notification %s: %s",
                notification_id,
                exc,
            )

        except Exception as db_exc:
            logger.exception(
                "Could not persist permanent provider failure"
            )

            await schedule_retry(
                message,
                publisher_channel,
                retry_count,
                str(db_exc),
                notification_id,
            )

        return

    except (TransientProviderError, Exception) as exc:

        logger.exception(
            "SMS provider submission failed for notification %s",
            notification_id,
        )

        # Best-effort recording of the latest provider error.
        try:
            with SessionLocal.begin() as db:
                notification = db.get(
                    Notification,
                    notification_id,
                )

                if notification is not None:
                    notification.provider_status = "retryable_error"
                    notification.failure_reason = str(exc)[:2000]

        except Exception:
            logger.exception(
                "Could not persist provider error details"
            )

        await schedule_retry(
            message,
            publisher_channel,
            retry_count,
            str(exc),
            notification_id,
        )
        return

    # 4. Provider accepted the SMS.
    # Save provider response and inbox record atomically.
    try:
        with SessionLocal.begin() as db:

            notification = db.execute(
                select(Notification)
                .where(Notification.id == notification_id)
                .with_for_update()
            ).scalar_one()

            if notification.status not in TERMINAL_STATUSES:
                notification.provider_name = result.provider_name
                notification.provider_message_id = (
                    result.provider_message_id
                )
                notification.provider_status = result.provider_status

                notification.status = "sent"
                notification.sent_at = datetime.now(timezone.utc)

                notification.failed_at = None
                notification.failure_reason = None

            insert_inbox_record(
                db,
                event_id,
                notification_id,
            )

        # Count only after the accepted state is committed.
        SMS_DELIVERY_RESULTS.labels(status="sent").inc()

        # Database commit completed before RabbitMQ ACK.
        await message.ack()

        logger.info(
            "Notification %s accepted by provider. "
            "Provider=%s, provider_message_id=%s",
            notification_id,
            result.provider_name,
            result.provider_message_id,
        )

    except Exception as exc:
        # Provider may already have accepted the SMS.
        # A retry must reuse the same provider idempotency key.
        logger.exception(
            "Provider accepted notification %s, "
            "but database persistence failed",
            notification_id,
        )

        await schedule_retry(
            message,
            publisher_channel,
            retry_count,
            str(exc),
            notification_id,
        )


async def main():

    # Expose metrics from this independent consumer process.
    metrics_host = os.getenv(
        "NOTIFYFLOW_SMS_CONSUMER_METRICS_HOST",
        "127.0.0.1",
    )
    metrics_port = int(
        os.getenv("NOTIFYFLOW_SMS_CONSUMER_METRICS_PORT", "9103")
    )

    start_http_server(port=metrics_port, addr=metrics_host)

    logger.info(
        "SMS consumer metrics available at http://%s:%s/metrics",
        metrics_host,
        metrics_port,
    )

    # One provider instance for this consumer process.
    provider = get_sms_provider()

    connection = await aio_pika.connect_robust(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        login=settings.RABBITMQ_USERNAME,
        password=settings.RABBITMQ_PASSWORD.get_secret_value(),
        virtualhost=settings.RABBITMQ_VHOST,
        timeout=10,
    )

    try:
        consumer_channel = await connection.channel()

        await consumer_channel.set_qos(
            prefetch_count=1,
        )

        publisher_channel = await connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )

        queue = await consumer_channel.declare_queue(
            MAIN_QUEUE,
            durable=True,
        )

        async def callback(message):
            started_at = perf_counter()

            try:
                await process_message(
                    message,
                    publisher_channel,
                    provider,
                )

            except Exception:
                SMS_CONSUMER_ERRORS.inc()
                logger.exception(
                    "Unexpected error while processing an SMS message"
                )
                raise

            finally:
                SMS_MESSAGE_PROCESSING_DURATION.observe(
                    perf_counter() - started_at
                )

        await queue.consume(callback)

        logger.info(
            "Consumer listening on: %s",
            MAIN_QUEUE,
        )
        logger.info(
            "SMS provider: %s",
            settings.SMS_PROVIDER,
        )
        logger.info(
            "Maximum processing attempts: %s",
            MAX_ATTEMPTS,
        )

        await asyncio.Future()

    finally:
        await connection.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Consumer stopped.")
