
import asyncio
import json
import logging
import os

from datetime import datetime, timedelta, timezone
from time import perf_counter

import aio_pika
from prometheus_client import start_http_server

from sqlalchemy import func, select

from app.database import SessionLocal, settings
from app.models import OutboxEvent
from app.worker_metrics import (
    SMS_OUTBOX_PUBLISH_ATTEMPTS,
    SMS_OUTBOX_PUBLISH_CONFIRMED,
    SMS_OUTBOX_PUBLISH_FAILURES,
    SMS_OUTBOX_PUBLISH_DURATION,
)


# RabbitMQ configuration
QUEUE_NAME = "notifyflow.notifications"

# Publisher configuration
BATCH_SIZE = 10
POLL_INTERVAL = 1
RECONNECT_DELAY = 5
PUBLISH_TIMEOUT = 10

MAX_RETRY_DELAY = 300  # 5 minutes


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("notifyflow.outbox_publisher")


async def publish_one(channel) -> bool:
    """
    Claim and publish one pending outbox event.

    Returns:
        True: An event was attempted.
        False: No eligible event was found.
    """

    with SessionLocal() as db:

        # Find one unpublished event that is ready for retry.
        # SKIP LOCKED allows multiple publisher processes
        # to work without claiming the same event.
        statement = (
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                OutboxEvent.next_attempt_at <= func.now(),
            )
            .order_by(
                OutboxEvent.created_at.asc(),
                OutboxEvent.id.asc(),
            )
            .limit(1)
            .with_for_update(
                skip_locked=True,
            )
        )

        event = db.execute(statement).scalar_one_or_none()

        if event is None:
            db.rollback()
            return False

        # Store these values before publishing.
        event_id = event.id
        event_payload = event.payload

        try:
            # Build a persistent RabbitMQ message.
            message = aio_pika.Message(
                body=json.dumps(
                    event_payload,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode("utf-8"),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=str(event_id),
                headers={
                    "event_type": event.event_type,
                },
            )

            # Publish through RabbitMQ's default exchange.
            # Wait for broker confirmation.
            # Measure the actual RabbitMQ publishing operation.
            publish_started_at = perf_counter()
            SMS_OUTBOX_PUBLISH_ATTEMPTS.inc()

            try:
                confirmation = await channel.default_exchange.publish(
                    message,
                    routing_key=QUEUE_NAME,
                    mandatory=True,
                    timeout=PUBLISH_TIMEOUT,
                )

                if confirmation is False:
                    raise RuntimeError(
                        "RabbitMQ did not confirm the message"
                    )

            except Exception:
                SMS_OUTBOX_PUBLISH_FAILURES.inc()
                raise

            finally:
                SMS_OUTBOX_PUBLISH_DURATION.observe(
                    perf_counter() - publish_started_at
                )

            # This measures broker confirmation, not database commit.
            SMS_OUTBOX_PUBLISH_CONFIRMED.inc()

        except Exception as exc:

            # Publication failed. Record the failure.
            event.attempts += 1

            # Exponential backoff:
            # 2, 4, 8, 16 ... up to 300 seconds.
            exponent = min(event.attempts, 9)

            retry_delay = min(
                2 ** exponent,
                MAX_RETRY_DELAY,
            )

            event.next_attempt_at = (
                datetime.now(timezone.utc)
                + timedelta(seconds=retry_delay)
            )

            event.last_error = str(exc)[:2000]

            # Persist retry information and release the row lock.
            db.commit()

            logger.warning(
                "Event %s publishing failed. "
                "Attempt=%s, retry_in=%ss, error=%s",
                event_id,
                event.attempts,
                retry_delay,
                exc,
            )

            return True

        # RabbitMQ confirmed the publication.
        event.published_at = datetime.now(timezone.utc)
        event.last_error = None

        # Persist the successful publishing status.
        db.commit()

        logger.info(
            "Event %s successfully published to RabbitMQ",
            event_id,
        )

        return True


async def publish_batch(channel) -> int:
    """
    Attempt to publish up to BATCH_SIZE events.

    Returns the number of events attempted.
    """

    attempted = 0

    for _ in range(BATCH_SIZE):

        processed = await publish_one(channel)

        if not processed:
            break

        attempted += 1

    return attempted


async def main():

    metrics_host = os.getenv(
        "NOTIFYFLOW_SMS_PUBLISHER_METRICS_HOST",
        "127.0.0.1",
    )
    metrics_port = int(
        os.getenv("NOTIFYFLOW_SMS_PUBLISHER_METRICS_PORT", "9104")
    )

    start_http_server(port=metrics_port, addr=metrics_host)

    logger.info(
        "SMS publisher metrics available at http://%s:%s/metrics",
        metrics_host,
        metrics_port,
    )

    while True:

        connection = None

        try:
            logger.info("Connecting to RabbitMQ...")

            connection = await aio_pika.connect_robust(
                host=settings.RABBITMQ_HOST,
                port=settings.RABBITMQ_PORT,
                login=settings.RABBITMQ_USERNAME,
                password=settings.RABBITMQ_PASSWORD.get_secret_value(),
                virtualhost=settings.RABBITMQ_VHOST,
                timeout=PUBLISH_TIMEOUT,
            )

            # Enable publisher confirms and raise errors
            # for mandatory messages that cannot be routed.
            channel = await connection.channel(
                publisher_confirms=True,
                on_return_raises=True,
            )

            # Ensure the destination queue exists.
            await channel.declare_queue(
                QUEUE_NAME,
                durable=True,
            )

            logger.info(
                "Connected to RabbitMQ. Publisher is running."
            )

            while True:

                attempted = await publish_batch(channel)

                # Sleep only when no full batch was attempted.
                # If a full batch was processed, immediately
                # check for more pending events.
                if attempted < BATCH_SIZE:
                    await asyncio.sleep(POLL_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Publisher shutdown requested.")
            raise

        except Exception:
            logger.exception(
                "Publisher encountered an error. "
                "Reconnecting in %s seconds...",
                RECONNECT_DELAY,
            )

            await asyncio.sleep(RECONNECT_DELAY)

        finally:
            if connection is not None and not connection.is_closed:
                try:
                    await connection.close()
                except Exception:
                    logger.exception(
                        "Error while closing RabbitMQ connection"
                    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Publisher stopped.")
