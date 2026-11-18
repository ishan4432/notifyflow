import asyncio

import aio_pika

from app.database import settings


MAIN_QUEUE = "notifyflow.notifications"
RETRY_QUEUE = "notifyflow.retry"
DLQ_QUEUE = "notifyflow.dlq"

RETRY_DELAY_MS = 5000


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
        channel = await connection.channel(
            publisher_confirms=True,
        )

        # Existing main queue.
        await channel.declare_queue(
            MAIN_QUEUE,
            durable=True,
        )

        # Failed messages wait here for 5 seconds.
        # After expiry, RabbitMQ routes them back
        # to the main queue using the default exchange.
        await channel.declare_queue(
            RETRY_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": RETRY_DELAY_MS,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": MAIN_QUEUE,
            },
        )

        # Permanently failed messages are stored here.
        await channel.declare_queue(
            DLQ_QUEUE,
            durable=True,
        )

        print("RabbitMQ topology configured successfully!")
        print(f"Main queue: {MAIN_QUEUE}")
        print(f"Retry queue: {RETRY_QUEUE}")
        print(f"Retry delay: {RETRY_DELAY_MS} ms")
        print(f"Dead-letter queue: {DLQ_QUEUE}")

    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
