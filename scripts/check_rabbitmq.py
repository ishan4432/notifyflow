
import asyncio

import aio_pika

from app.database import settings


async def main():
    connection = await aio_pika.connect_robust(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        login=settings.RABBITMQ_USERNAME,
        password=settings.RABBITMQ_PASSWORD.get_secret_value(),
        virtualhost=settings.RABBITMQ_VHOST,
        timeout=10,
        client_properties={
            "connection_name": "notifyflow-connection-test"
        },
    )

    try:
        print("Successfully connected to RabbitMQ!")
        print(f"Connected virtual host: {settings.RABBITMQ_VHOST}")
    finally:
        await connection.close()
        print("RabbitMQ connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
