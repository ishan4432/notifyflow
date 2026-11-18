
import argparse
import asyncio

import aio_pika

from app.database import SessionLocal, settings
from app.models import OutboxEvent


QUEUE_NAME = "notifyflow.notifications"


async def main(event_id: int):
    # Read the original event without modifying it.
    with SessionLocal() as db:
        event = db.get(OutboxEvent, event_id)

        if event is None:
            raise ValueError(f"Outbox event {event_id} not found")

        payload = event.payload
        event_type = event.event_type

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

        await channel.declare_queue(
            QUEUE_NAME,
            durable=True,
        )

        message = aio_pika.Message(
            body=__import__("json").dumps(payload).encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,

            # Reuse the original event ID!
            message_id=str(event_id),

            headers={"event_type": event_type},
        )

        confirmation = await channel.default_exchange.publish(
            message,
            routing_key=QUEUE_NAME,
            mandatory=True,
            timeout=10,
        )

        if confirmation is False:
            raise RuntimeError("RabbitMQ did not confirm replay")

        print(f"Event {event_id} replayed successfully.")
        print(f"Message ID: {event_id}")

    finally:
        await connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("event_id", type=int)
    args = parser.parse_args()

    asyncio.run(main(args.event_id))
