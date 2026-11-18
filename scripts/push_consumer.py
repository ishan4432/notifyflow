
import asyncio
import json
import logging
import os

from datetime import datetime, timedelta, timezone
from time import perf_counter

import aio_pika
from prometheus_client import start_http_server

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import SessionLocal, settings
from app.models import (
    PushNotification,
    PushSubscription,
    PushDelivery,
)
from app.providers.web_push_factory import get_web_push_provider
from app.providers.web_push_provider import (
    ExpiredPushSubscriptionError,
    PermanentPushError,
    TransientPushError,
)
from app.worker_metrics import (
    PUSH_PROVIDER_ATTEMPTS,
    PUSH_DELIVERY_RESULTS,
    PUSH_RECOVERY_SCHEDULED,
    PUSH_DLQ_EVENTS,
    PUSH_MESSAGE_PROCESSING_DURATION,
    PUSH_CONSUMER_ERRORS,
)


# ---------------------------------------------------------
# RabbitMQ configuration
# ---------------------------------------------------------

MAIN_QUEUE = "notifyflow.push.notifications"
RETRY_QUEUE = "notifyflow.push.retry"
RECOVERY_QUEUE = "notifyflow.push.recovery"
DLQ_QUEUE = "notifyflow.push.dlq"

MAX_ATTEMPTS = 3
RETRY_COUNT_HEADER = "x-retry-count"

PUBLISH_TIMEOUT = 10

# A delivery left in processing for this long can be reclaimed.
PUSH_DELIVERY_LEASE_SECONDS = 120

# Recovery messages return after the processing lease expires,
# with a small additional buffer.
RECOVERY_DELAY_MS = (
    PUSH_DELIVERY_LEASE_SECONDS + 5
) * 1000


TERMINAL_NOTIFICATION_STATUSES = {
    "accepted",
    "partial_failure",
    "failed",
    "no_subscribers",
}


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("notifyflow.push_consumer")


# ---------------------------------------------------------
# Exceptions and message helpers
# ---------------------------------------------------------

class InvalidPushMessage(Exception):
    """Raised when an incoming push event is invalid."""


def get_retry_count(message):
    headers = message.headers or {}
    value = headers.get(RETRY_COUNT_HEADER, 0)

    try:
        count = int(value)
    except (TypeError, ValueError):
        raise InvalidPushMessage("Invalid retry count")

    if count < 0:
        raise InvalidPushMessage("Retry count cannot be negative")

    return count


# ---------------------------------------------------------
# Notification fan-out
# ---------------------------------------------------------

def prepare_push_notification(push_notification_id):
    """
    Create one PushDelivery per active subscription.

    Fan-out happens only once, while the notification is queued.
    The notification and delivery records are committed together.
    """

    with SessionLocal.begin() as db:

        notification = db.execute(
            select(PushNotification)
            .where(
                PushNotification.id == push_notification_id
            )
            .with_for_update()
        ).scalar_one_or_none()

        if notification is None:
            raise InvalidPushMessage(
                f"Push notification {push_notification_id} not found"
            )

        if notification.status in TERMINAL_NOTIFICATION_STATUSES:
            return "terminal"

        if notification.status == "processing":
            # Existing delivery records are retained on retries.
            return "ready"

        if notification.status != "queued":
            raise InvalidPushMessage(
                f"Unsupported push notification status: "
                f"{notification.status}"
            )

        subscriptions = db.execute(
            select(PushSubscription).where(
                PushSubscription.expired_at.is_(None)
            )
        ).scalars().all()

        if not subscriptions:
            notification.status = "no_subscribers"
            notification.completed_at = datetime.now(timezone.utc)

            logger.info(
                "Push notification %s has no active subscriptions",
                push_notification_id,
            )

            return "no_subscribers"

        delivery_rows = [
            {
                "push_notification_id": push_notification_id,
                "push_subscription_id": subscription.id,
                "status": "queued",
                "attempts": 0,
            }
            for subscription in subscriptions
        ]

        statement = (
            pg_insert(PushDelivery)
            .values(delivery_rows)
            .on_conflict_do_nothing(
                index_elements=[
                    PushDelivery.push_notification_id,
                    PushDelivery.push_subscription_id,
                ]
            )
        )

        db.execute(statement)

        notification.status = "processing"

        logger.info(
            "Created delivery records for notification %s. "
            "Active subscriptions=%s",
            push_notification_id,
            len(subscriptions),
        )

    return "ready"


# ---------------------------------------------------------
# Delivery lease and atomic claim
# ---------------------------------------------------------

def get_delivery_ids(push_notification_id, redelivered):
    """
    Find deliveries eligible for processing.

    Queued and retrying deliveries are immediately eligible.

    Processing deliveries are eligible when:
      - RabbitMQ redelivered the original unacknowledged event; or
      - Their processing lease has expired.
    """

    lease_cutoff = (
        datetime.now(timezone.utc)
        - timedelta(seconds=PUSH_DELIVERY_LEASE_SECONDS)
    )

    eligible_conditions = [
        PushDelivery.status.in_(["queued", "retrying"])
    ]

    if redelivered:
        eligible_conditions.append(
            PushDelivery.status == "processing"
        )
    else:
        eligible_conditions.append(
            and_(
                PushDelivery.status == "processing",
                PushDelivery.updated_at <= lease_cutoff,
            )
        )

    with SessionLocal() as db:

        statement = (
            select(PushDelivery.id)
            .where(
                PushDelivery.push_notification_id == push_notification_id,
                or_(*eligible_conditions),
            )
            .order_by(PushDelivery.id.asc())
        )

        return list(
            db.execute(statement).scalars().all()
        )


def claim_delivery(delivery_id, redelivered):
    """
    Atomically claim an eligible delivery.

    The conditional UPDATE prevents concurrent workers from
    claiming the same queued or expired delivery.
    """

    lease_cutoff = (
        datetime.now(timezone.utc)
        - timedelta(seconds=PUSH_DELIVERY_LEASE_SECONDS)
    )

    eligible_conditions = [
        PushDelivery.status.in_(["queued", "retrying"])
    ]

    if redelivered:
        eligible_conditions.append(
            PushDelivery.status == "processing"
        )
    else:
        eligible_conditions.append(
            and_(
                PushDelivery.status == "processing",
                PushDelivery.updated_at <= lease_cutoff,
            )
        )

    with SessionLocal.begin() as db:

        statement = (
            update(PushDelivery)
            .where(
                PushDelivery.id == delivery_id,
                or_(*eligible_conditions),
            )
            .values(
                status="processing",
                attempts=PushDelivery.attempts + 1,
                updated_at=func.now(),
            )
            .returning(
                PushDelivery.push_subscription_id,
                PushDelivery.attempts,
            )
        )

        claimed = db.execute(statement).one_or_none()

        if claimed is None:
            return None

        subscription_id, attempts = claimed

        subscription = db.get(
            PushSubscription,
            subscription_id,
        )

        if subscription is None:
            delivery = db.get(
                PushDelivery,
                delivery_id,
            )

            delivery.status = "failed"
            delivery.last_error = "Push subscription not found"

            return None

        if subscription.expired_at is not None:
            delivery = db.get(
                PushDelivery,
                delivery_id,
            )

            delivery.status = "expired"
            delivery.last_error = "Push subscription already expired"

            return None

        subscription_info = {
            "endpoint": subscription.endpoint,
            "keys": {
                "p256dh": subscription.p256dh_key,
                "auth": subscription.auth_key,
            },
        }

        return {
            "subscription_id": subscription_id,
            "attempts": attempts,
            "subscription_info": subscription_info,
        }


# ---------------------------------------------------------
# Delivery result persistence
# ---------------------------------------------------------

def save_delivery_result(
    delivery_id,
    result_status,
    last_error=None,
    status_code=None,
    expire_subscription=False,
):
    """
    Persist an individual subscription delivery result.
    """

    now = datetime.now(timezone.utc)

    with SessionLocal.begin() as db:

        delivery = db.get(
            PushDelivery,
            delivery_id,
        )

        if delivery is None:
            raise RuntimeError(
                f"Push delivery {delivery_id} not found"
            )

        delivery.status = result_status
        delivery.last_error = (
            last_error[:2000] if last_error else None
        )
        delivery.push_status_code = status_code
        delivery.updated_at = now

        if result_status == "accepted":
            delivery.accepted_at = now

        if expire_subscription:
            subscription = db.get(
                PushSubscription,
                delivery.push_subscription_id,
            )

            if subscription is not None:
                subscription.expired_at = now

            logger.warning(
                "Push subscription %s expired",
                delivery.push_subscription_id,
            )

    # Record the result only after the database transaction commits.
    PUSH_DELIVERY_RESULTS.labels(status=result_status).inc()


# ---------------------------------------------------------
# Push delivery fan-out
# ---------------------------------------------------------

async def deliver_pending(
    push_notification_id,
    title,
    body,
    target_url,
    provider,
    redelivered,
):
    """
    Deliver to each eligible browser subscription independently.
    """

    delivery_ids = get_delivery_ids(
        push_notification_id,
        redelivered,
    )

    for delivery_id in delivery_ids:

        claim = claim_delivery(
            delivery_id,
            redelivered,
        )

        if claim is None:
            continue

        payload = {
            "title": title,
            "body": body,
            "url": target_url,
            "notification_id": push_notification_id,
        }

        # Count an attempt immediately before calling the provider.
        PUSH_PROVIDER_ATTEMPTS.inc()

        try:
            result = await provider.send(
                subscription_info=claim["subscription_info"],
                payload=payload,
            )

        except ExpiredPushSubscriptionError as exc:

            save_delivery_result(
                delivery_id,
                result_status="expired",
                last_error=str(exc),
                expire_subscription=True,
            )

            continue

        except TransientPushError as exc:

            save_delivery_result(
                delivery_id,
                result_status="retrying",
                last_error=str(exc),
            )

            logger.warning(
                "Temporary push failure. Notification=%s "
                "Delivery=%s Attempt=%s Error=%s",
                push_notification_id,
                delivery_id,
                claim["attempts"],
                exc,
            )

            continue

        except PermanentPushError as exc:

            save_delivery_result(
                delivery_id,
                result_status="failed",
                last_error=str(exc),
            )

            logger.error(
                "Permanent push failure. Notification=%s "
                "Delivery=%s Error=%s",
                push_notification_id,
                delivery_id,
                exc,
            )

            continue

        except Exception as exc:

            # Unexpected failures are treated as retryable.
            logger.exception(
                "Unexpected push error for delivery %s",
                delivery_id,
            )

            save_delivery_result(
                delivery_id,
                result_status="retrying",
                last_error=str(exc),
            )

            continue

        save_delivery_result(
            delivery_id,
            result_status="accepted",
            status_code=result.status_code,
        )

        logger.info(
            "Push accepted by push service. "
            "Notification=%s Delivery=%s HTTP=%s",
            push_notification_id,
            delivery_id,
            result.status_code,
        )


# ---------------------------------------------------------
# Delivery summary and final notification status
# ---------------------------------------------------------

def get_delivery_summary(push_notification_id):

    with SessionLocal() as db:

        rows = db.execute(
            select(
                PushDelivery.status,
                func.count(PushDelivery.id),
            )
            .where(
                PushDelivery.push_notification_id == push_notification_id
            )
            .group_by(PushDelivery.status)
        ).all()

        return {
            delivery_status: count
            for delivery_status, count in rows
        }


def finalize_push_notification(push_notification_id):
    """
    Finalize only when no delivery remains queued, retrying,
    or processing.
    """

    with SessionLocal.begin() as db:

        notification = db.execute(
            select(PushNotification)
            .where(
                PushNotification.id == push_notification_id
            )
            .with_for_update()
        ).scalar_one_or_none()

        if notification is None:
            raise InvalidPushMessage(
                "Push notification disappeared during processing"
            )

        summary_rows = db.execute(
            select(
                PushDelivery.status,
                func.count(PushDelivery.id),
            )
            .where(
                PushDelivery.push_notification_id == push_notification_id
            )
            .group_by(PushDelivery.status)
        ).all()

        counts = {
            delivery_status: count
            for delivery_status, count in summary_rows
        }

        pending = sum(
            counts.get(value, 0)
            for value in ("queued", "retrying", "processing")
        )

        if pending:
            return None

        total = sum(counts.values())

        if total == 0:
            final_status = "no_subscribers"

        else:
            accepted = counts.get("accepted", 0)

            unsuccessful = (
                counts.get("failed", 0)
                + counts.get("expired", 0)
            )

            if accepted and unsuccessful:
                final_status = "partial_failure"
            elif accepted:
                final_status = "accepted"
            else:
                final_status = "failed"

        notification.status = final_status
        notification.completed_at = datetime.now(timezone.utc)

        logger.info(
            "Push notification %s finalized. Status=%s Summary=%s",
            push_notification_id,
            final_status,
            counts,
        )

        return final_status


def mark_pending_deliveries_failed(
    push_notification_id,
    reason,
):
    """
    Mark queued and retrying deliveries as failed after
    the normal provider retry limit is exhausted.

    Active processing deliveries are not forcibly changed:
    they are handled by the recovery lease.
    """

    with SessionLocal.begin() as db:

        failed_delivery_ids = db.execute(
            update(PushDelivery)
            .where(
                PushDelivery.push_notification_id == push_notification_id,
                PushDelivery.status.in_(["queued", "retrying"]),
            )
            .values(
                status="failed",
                last_error=reason[:2000],
                updated_at=func.now(),
            )
            .returning(PushDelivery.id)
        ).scalars().all()

    # Record bulk failures only after the transaction commits.
    if failed_delivery_ids:
        PUSH_DELIVERY_RESULTS.labels(status="failed").inc(
            len(failed_delivery_ids)
        )


# ---------------------------------------------------------
# RabbitMQ message forwarding
# ---------------------------------------------------------

async def publish_copy(
    message,
    publisher_channel,
    destination,
    extra_headers,
):
    headers = dict(message.headers or {})

    # Remove previous dead-letter history.
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
            "Could not forward push event %s to %s",
            getattr(message, "message_id", "unknown"),
            destination,
        )

        await message.nack(requeue=True)
        return False

    # ACK only after the broker confirms the copy.
    await message.ack()
    return True


async def send_to_dlq(
    message,
    publisher_channel,
    reason,
):
    forwarded = await forward_and_ack(
        message,
        publisher_channel,
        DLQ_QUEUE,
        {
            "x-failure-reason": reason[:1000],
        },
    )

    if forwarded:
        PUSH_DLQ_EVENTS.inc()

        logger.error(
            "Push event %s moved to DLQ: %s",
            getattr(message, "message_id", "unknown"),
            reason,
        )


# ---------------------------------------------------------
# Delayed processing-lease recovery
# ---------------------------------------------------------

async def schedule_recovery(
    message,
    publisher_channel,
    reason,
):
    """
    Schedule another inspection after the processing lease expires.

    Recovery does not consume the normal provider retry budget.
    """

    forwarded = await forward_and_ack(
        message,
        publisher_channel,
        RECOVERY_QUEUE,
        {
            RETRY_COUNT_HEADER: 0,
            "x-recovery-reason": reason[:1000],
        },
    )

    if forwarded:
        PUSH_RECOVERY_SCHEDULED.inc()

        logger.warning(
            "Push event %s scheduled for lease recovery. "
            "Recovery delay=%s seconds. Reason=%s",
            getattr(message, "message_id", "unknown"),
            RECOVERY_DELAY_MS // 1000,
            reason,
        )

    return forwarded


# ---------------------------------------------------------
# Normal retry and dead-letter handling
# ---------------------------------------------------------

async def schedule_retry(
    message,
    publisher_channel,
    retry_count,
    reason,
    push_notification_id=None,
):
    attempt_number = retry_count + 1

    if attempt_number >= MAX_ATTEMPTS:

        if push_notification_id is not None:

            mark_pending_deliveries_failed(
                push_notification_id,
                reason,
            )

            final_status = finalize_push_notification(
                push_notification_id,
            )

            # A separate worker may still own a processing delivery.
            # Do not DLQ the event while that delivery remains unresolved.
            if final_status is None:
                await schedule_recovery(
                    message,
                    publisher_channel,
                    (
                        "Normal retry limit reached, but one or more "
                        "deliveries remain processing"
                    ),
                )
                return

        await send_to_dlq(
            message,
            publisher_channel,
            (
                f"Maximum push processing attempts reached: "
                f"{MAX_ATTEMPTS}. Last error: {reason}"
            ),
        )

        return

    forwarded = await forward_and_ack(
        message,
        publisher_channel,
        RETRY_QUEUE,
        {
            RETRY_COUNT_HEADER: attempt_number,
        },
    )

    if forwarded:
        logger.warning(
            "Push event %s failed on attempt %s. "
            "Scheduled retry %s.",
            getattr(message, "message_id", "unknown"),
            attempt_number,
            attempt_number,
        )


# ---------------------------------------------------------
# Main message processing
# ---------------------------------------------------------

async def process_message(
    message,
    publisher_channel,
    provider,
):
    event_id = message.message_id
    push_notification_id = None

    # Step 1: Validate incoming RabbitMQ event.
    try:
        if not event_id:
            raise InvalidPushMessage("Missing message_id")

        retry_count = get_retry_count(message)

        payload = json.loads(
            message.body.decode("utf-8")
        )

        if not isinstance(payload, dict):
            raise InvalidPushMessage(
                "Push event payload must be a JSON object"
            )

        push_notification_id = payload.get(
            "push_notification_id"
        )

        if (
            type(push_notification_id) is not int
            or push_notification_id <= 0
        ):
            raise InvalidPushMessage(
                "Invalid push_notification_id"
            )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        InvalidPushMessage,
    ) as exc:

        await send_to_dlq(
            message,
            publisher_channel,
            str(exc),
        )

        return

    redelivered = bool(
        getattr(message, "redelivered", False)
    )

    # Step 2: Prepare notification and fan-out records.
    try:
        preparation_result = prepare_push_notification(
            push_notification_id
        )

        if preparation_result in {"terminal", "no_subscribers"}:
            await message.ack()
            return

    except InvalidPushMessage as exc:

        await send_to_dlq(
            message,
            publisher_channel,
            str(exc),
        )

        return

    except Exception as exc:

        logger.exception(
            "Could not prepare push notification %s",
            push_notification_id,
        )

        await schedule_retry(
            message,
            publisher_channel,
            retry_count,
            str(exc),
            push_notification_id,
        )

        return

    # Step 3: Load notification content from PostgreSQL.
    try:
        with SessionLocal() as db:

            notification = db.get(
                PushNotification,
                push_notification_id,
            )

            if notification is None:
                raise InvalidPushMessage(
                    "Push notification not found"
                )

            title = notification.title
            body = notification.body
            target_url = notification.target_url

    except InvalidPushMessage as exc:

        await send_to_dlq(
            message,
            publisher_channel,
            str(exc),
        )

        return

    except Exception as exc:

        logger.exception(
            "Could not load push notification %s",
            push_notification_id,
        )

        await schedule_retry(
            message,
            publisher_channel,
            retry_count,
            str(exc),
            push_notification_id,
        )

        return

    # Step 4: Deliver independently to eligible subscriptions.
    try:
        await deliver_pending(
            push_notification_id=push_notification_id,
            title=title,
            body=body,
            target_url=target_url,
            provider=provider,
            redelivered=redelivered,
        )

    except Exception as exc:

        logger.exception(
            "Push delivery processing failed for notification %s",
            push_notification_id,
        )

        await schedule_retry(
            message,
            publisher_channel,
            retry_count,
            str(exc),
            push_notification_id,
        )

        return

    # Step 5: Inspect delivery states.
    try:
        summary = get_delivery_summary(
            push_notification_id
        )

    except Exception as exc:

        logger.exception(
            "Could not inspect push delivery states"
        )

        await schedule_retry(
            message,
            publisher_channel,
            retry_count,
            str(exc),
            push_notification_id,
        )

        return

    retryable = (
        summary.get("queued", 0)
        + summary.get("retrying", 0)
    )

    processing = summary.get("processing", 0)

    # Prioritize retryable provider failures.
    if retryable:

        await schedule_retry(
            message,
            publisher_channel,
            retry_count,
            "One or more push deliveries require retry",
            push_notification_id,
        )

        return

    # Active or recently claimed deliveries are not stolen.
    # A recovery message will inspect them after the lease.
    if processing:

        await schedule_recovery(
            message,
            publisher_channel,
            (
                f"{processing} deliveries remain processing"
            ),
        )

        return

    # Step 6: Finalize notification aggregate status.
    try:
        final_status = finalize_push_notification(
            push_notification_id
        )

    except Exception as exc:

        logger.exception(
            "Could not finalize push notification %s",
            push_notification_id,
        )

        await schedule_retry(
            message,
            publisher_channel,
            retry_count,
            str(exc),
            push_notification_id,
        )

        return

    if final_status is None:

        await schedule_recovery(
            message,
            publisher_channel,
            "Deliveries were not ready for finalization",
        )

        return

    # Step 7: ACK only after database state is committed.
    await message.ack()

    logger.info(
        "Push event %s completed. Notification=%s Status=%s",
        event_id,
        push_notification_id,
        final_status,
    )


# ---------------------------------------------------------
async def handle_push_message(
    message,
    publisher_channel,
    provider,
):
    started_at = perf_counter()

    try:
        await process_message(
            message,
            publisher_channel,
            provider,
        )

    except Exception:
        PUSH_CONSUMER_ERRORS.inc()

        logger.exception(
            "Unhandled push consumer error"
        )

        if not message.processed:
            await message.nack(requeue=True)

    finally:
        PUSH_MESSAGE_PROCESSING_DURATION.observe(
            perf_counter() - started_at
        )


# Consumer startup
# ---------------------------------------------------------

async def main():

    # Metrics belong to this consumer process, not the FastAPI process.
    metrics_host = os.getenv(
        "NOTIFYFLOW_PUSH_METRICS_HOST",
        "127.0.0.1",
    )
    metrics_port = int(
        os.getenv("NOTIFYFLOW_PUSH_METRICS_PORT", "9101")
    )

    start_http_server(
        port=metrics_port,
        addr=metrics_host,
    )

    logger.info(
        "Push consumer metrics available at http://%s:%s/metrics",
        metrics_host,
        metrics_port,
    )

    provider = get_web_push_provider()

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

        await consumer_channel.declare_queue(
            MAIN_QUEUE,
            durable=True,
        )

        # Declare the delayed recovery queue. Messages return
        # to the main queue after their TTL expires.
        await consumer_channel.declare_queue(
            RECOVERY_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": RECOVERY_DELAY_MS,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": MAIN_QUEUE,
            },
        )

        queue = await consumer_channel.declare_queue(
            MAIN_QUEUE,
            durable=True,
        )

        async def callback(message):
            await handle_push_message(
                message,
                publisher_channel,
                provider,
            )

        await queue.consume(callback)

        logger.info(
            "Push consumer listening on %s",
            MAIN_QUEUE,
        )

        logger.info(
            "Maximum provider retry attempts: %s",
            MAX_ATTEMPTS,
        )

        logger.info(
            "Processing lease: %s seconds",
            PUSH_DELIVERY_LEASE_SECONDS,
        )

        logger.info(
            "Recovery queue: %s",
            RECOVERY_QUEUE,
        )

        await asyncio.Future()

    finally:
        await connection.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Push consumer stopped.")
