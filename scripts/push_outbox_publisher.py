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
from app.models import PushOutboxEvent
from app.worker_metrics import (
    PUSH_OUTBOX_PUBLISH_ATTEMPTS,
    PUSH_OUTBOX_PUBLISH_CONFIRMED,
    PUSH_OUTBOX_PUBLISH_FAILURES,
    PUSH_OUTBOX_PUBLISH_DURATION,
)


# Dedicated push queue; SMS publisher remains unchanged.
QUEUE_NAME = "notifyflow.push.notifications"

BATCH_SIZE = 10
POLL_INTERVAL = 1
RECONNECT_DELAY = 5
PUBLISH_TIMEOUT = 10

MAX_RETRY_DELAY = 300


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("notifyflow.push_outbox_publisher")


async def publish_one(channel) -> bool:
    """
    Claim and publish one eligible push outbox event.

    Returns:
        True: An event was attempted.
        False: No eligible event was found.
    """

    with SessionLocal() as db:

        statement = (
            select(PushOutboxEvent)
            .where(
                PushOutboxEvent.published_at.is_(None),
                PushOutboxEvent.next_attempt_at <= func.now(),
            )
            .order_by(
                PushOutboxEvent.created_at.asc(),
                PushOutboxEvent.id.asc(),
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )

        event = db.execute(statement).scalar_one_or_none()

        if event is None:
            db.rollback()
            return False

        event_id = event.id
        event_payload = event.payload

        try:
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
                    "channel": "webpush",
                },
            )

            # Measure the actual broker publishing operation.
            publish_started_at = perf_counter()
            PUSH_OUTBOX_PUBLISH_ATTEMPTS.inc()

            try:
                confirmation = await channel.default_exchange.publish(
                    message,
                    routing_key=QUEUE_NAME,
                    mandatory=True,
                    timeout=PUBLISH_TIMEOUT,
                )

                if confirmation is False:
                    raise RuntimeError(
                        "RabbitMQ did not confirm the push event"
                    )

            except Exception:
                PUSH_OUTBOX_PUBLISH_FAILURES.inc()
                raise

            finally:
                PUSH_OUTBOX_PUBLISH_DURATION.observe(
                    perf_counter() - publish_started_at
                )

            # Count only after RabbitMQ confirms publication.
            PUSH_OUTBOX_PUBLISH_CONFIRMED.inc()

        except Exception as exc:

            event.attempts += 1

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

            db.commit()

            logger.warning(
                "Push event %s publishing failed. "
                "Attempt=%s, retry_in=%ss, error=%s",
                event_id,
                event.attempts,
                retry_delay,
                exc,
            )

            return True

        event.published_at = datetime.now(timezone.utc)
        event.last_error = None

        db.commit()

        logger.info(
            "Push event %s successfully published to RabbitMQ",
            event_id,
        )

        return True


async def publish_batch(channel) -> int:
    attempted = 0

    for _ in range(BATCH_SIZE):
        processed = await publish_one(channel)

        if not processed:
            break

        attempted += 1

    return attempted


async def main():

    # The publisher is a separate process with its own metrics registry.
    metrics_host = os.getenv(
        "NOTIFYFLOW_PUSH_PUBLISHER_METRICS_HOST",
        "127.0.0.1",
    )
    metrics_port = int(
        os.getenv("NOTIFYFLOW_PUSH_PUBLISHER_METRICS_PORT", "9102")
    )

    start_http_server(
        port=metrics_port,
        addr=metrics_host,
    )

    logger.info(
        "Push publisher metrics available at http://%s:%s/metrics",
        metrics_host,
        metrics_port,
    )

    while True:
        connection = None

        try:
            logger.info("Connecting to RabbitMQ for push publishing...")

            connection = await aio_pika.connect_robust(
                host=settings.RABBITMQ_HOST,
                port=settings.RABBITMQ_PORT,
                login=settings.RABBITMQ_USERNAME,
                password=settings.RABBITMQ_PASSWORD.get_secret_value(),
                virtualhost=settings.RABBITMQ_VHOST,
                timeout=PUBLISH_TIMEOUT,
            )

            channel = await connection.channel(
                publisher_confirms=True,
                on_return_raises=True,
            )

            await channel.declare_queue(
                QUEUE_NAME,
                durable=True,
            )

            logger.info(
                "Push outbox publisher connected. Queue=%s",
                QUEUE_NAME,
            )

            while True:
                attempted = await publish_batch(channel)

                if attempted < BATCH_SIZE:
                    await asyncio.sleep(POLL_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Push publisher shutdown requested.")
            raise

        except Exception:
            logger.exception(
                "Push publisher encountered an error. "
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
                        "Error closing push publisher connection"
                    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Push publisher stopped.")
