
import argparse
import asyncio

import aio_pika

from app.database import settings


MAIN_QUEUE = "notifyflow.notifications"
DLQ_QUEUE = "notifyflow.dlq"


async def main(event_id: str):

    connection = await aio_pika.connect_robust(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        login=settings.RABBITMQ_USERNAME,
        password=settings.RABBITMQ_PASSWORD.get_secret_value(),
        virtualhost=settings.RABBITMQ_VHOST,
        timeout=10,
    )

    try:
        channel = await connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )

        dlq = await channel.declare_queue(
            DLQ_QUEUE,
            durable=True,
        )

        await channel.declare_queue(
            MAIN_QUEUE,
            durable=True,
        )

        # Retrieve the next available DLQ message.
        original = await dlq.get(
            no_ack=False,
            fail=False,
        )

        if original is None:
            print("Dead-Letter Queue is empty.")
            return

        # Only replay the requested event.
        if str(original.message_id) != event_id:
            await original.nack(requeue=True)

            raise RuntimeError(
                f"Next DLQ message is event {original.message_id}, "
                f"not requested event {event_id}. "
                "Original message was returned to the DLQ."
            )

        # Preserve useful metadata but reset previous failure history.
        headers = dict(original.headers or {})

        headers.pop("x-retry-count", None)
        headers.pop("x-failure-reason", None)

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

        # This is a fresh administrative replay.
        headers["x-retry-count"] = 0

        replay_message = aio_pika.Message(
            body=original.body,
            headers=headers,
            content_type=original.content_type or "application/json",
            content_encoding=original.content_encoding,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=original.message_id,
            correlation_id=original.correlation_id,
            priority=original.priority,
        )

        # Publish to the main queue and wait for confirmation.
        try:
            confirmation = await channel.default_exchange.publish(
                replay_message,
                routing_key=MAIN_QUEUE,
                mandatory=True,
                timeout=10,
            )

            if confirmation is False:
                raise RuntimeError(
                    "RabbitMQ did not confirm the replay"
                )

        except Exception:
            # Keep the original DLQ message if replay fails.
            await original.nack(requeue=True)
            raise

        # Remove the original only after successful publication.
        await original.ack()

        print("\nDLQ replay successful!")
        print(f"Original event ID: {event_id}")
        print(f"Destination: {MAIN_QUEUE}")
        print("Retry count reset to 0.")
        print("Original DLQ message acknowledged.")

    finally:
        await connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Replay a message from NotifyFlow DLQ."
    )

    parser.add_argument(
        "event_id",
        help="Original outbox event ID to replay",
    )

    args = parser.parse_args()

    try:
        asyncio.run(main(args.event_id))
    except KeyboardInterrupt:
        print("\nDLQ replay interrupted.")
