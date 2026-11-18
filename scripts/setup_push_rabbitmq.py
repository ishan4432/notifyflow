
import asyncio

import aio_pika

from app.database import settings


MAIN_QUEUE = "notifyflow.push.notifications"
RETRY_QUEUE = "notifyflow.push.retry"
RECOVERY_QUEUE = "notifyflow.push.recovery"
DLQ_QUEUE = "notifyflow.push.dlq"

RETRY_DELAY_MS = 5000

PUSH_DELIVERY_LEASE_SECONDS = 120
RECOVERY_DELAY_MS = (
    PUSH_DELIVERY_LEASE_SECONDS + 5
) * 1000


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

        await channel.declare_queue(
            MAIN_QUEUE,
            durable=True,
        )

        await channel.declare_queue(
            RETRY_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": RETRY_DELAY_MS,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": MAIN_QUEUE,
            },
        )

        await channel.declare_queue(
            RECOVERY_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": RECOVERY_DELAY_MS,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": MAIN_QUEUE,
            },
        )

        await channel.declare_queue(
            DLQ_QUEUE,
            durable=True,
        )

        print("Push RabbitMQ topology configured!")
        print(f"Main queue: {MAIN_QUEUE}")
        print(f"Retry queue: {RETRY_QUEUE}")
        print(f"Retry delay: {RETRY_DELAY_MS} ms")
        print(f"Recovery queue: {RECOVERY_QUEUE}")
        print(f"Recovery delay: {RECOVERY_DELAY_MS} ms")
        print(f"DLQ: {DLQ_QUEUE}")

    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
