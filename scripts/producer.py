
import asyncio
import json

import aio_pika

from app.database import settings


QUEUE_NAME = "notifyflow.notifications"


async def main():
    connection = await aio_pika.connect_robust(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        login=settings.RABBITMQ_USERNAME,
        password=settings.RABBITMQ_PASSWORD.get_secret_value(),
        virtualhost=settings.RABBITMQ_VHOST,
        timeout=10,
    )

    try:
        channel = await connection.channel()

        # Create a durable queue.
        queue = await channel.declare_queue(
            QUEUE_NAME,
            durable=True,
        )

        # Sample notification message.
        payload = {
            "notification_id": "demo-001",
            "recipient": "+919876543210",
            "message": "Hello from NotifyFlow RabbitMQ!",
            "priority": "high",
        }

        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id="demo-message-001",
        )

        # Publish message to RabbitMQ's default exchange.
        await channel.default_exchange.publish(
            message,
            routing_key=queue.name,
        )

        print("Message published successfully!")
        print(f"Queue: {queue.name}")
        print(f"Payload: {payload}")

    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
