
import asyncio
import json
import logging

import aio_pika

from app.database import settings


DLQ_QUEUE = "notifyflow.dlq"


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

        queue = await channel.declare_queue(
            DLQ_QUEUE,
            durable=True,
        )

        # Retrieve one message without acknowledging it.
        message = await queue.get(
            no_ack=False,
            fail=False,
        )

        if message is None:
            print("Dead-Letter Queue is empty.")
            return

        try:
            try:
                payload = json.loads(message.body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = {
                    "raw_body": message.body.decode(
                        "utf-8",
                        errors="replace",
                    )
                }

            headers = message.headers or {}

            print("\n========== DEAD-LETTER MESSAGE ==========")
            print(f"Message ID: {message.message_id}")
            print(f"Redelivered: {message.redelivered}")
            print(f"Event type: {headers.get('event_type')}")
            print(f"Retry count: {headers.get('x-retry-count', 0)}")
            print(f"Failure reason: {headers.get('x-failure-reason')}")
            print("-----------------------------------------")
            print(json.dumps(payload, indent=4))
            print("=========================================")

        finally:
            # Put the message back. Inspection must not delete it.
            await message.nack(requeue=True)

            print("\nMessage returned to DLQ.")
            print("No message was removed.")

    finally:
        await connection.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Inspection stopped.")
