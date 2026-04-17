import pika
import json
import time
from db import update_status


def callback(ch, method, properties, body):
    message = json.loads(body)
    msg_id = message["id"]

    try:
        print(f"Received: {message}")

        update_status(msg_id, "processing")

        time.sleep(2)

        if message.get("text") == "error":
            raise Exception("Test error")

        update_status(msg_id, "done")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print("Error:", e)

        update_status(msg_id, "failed")

        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_consumer():
    credentials = pika.PlainCredentials('guest', 'guest')

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='127.0.0.1',
                    credentials=credentials
                )
            )

            channel = connection.channel()
            channel.queue_declare(queue='test_queue', durable=True)

            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue='test_queue',
                on_message_callback=callback
            )

            print("✔ Consumer started")
            channel.start_consuming()

        except Exception as e:
            print("⚠ RabbitMQ not available, retrying...")
            time.sleep(3)


if __name__ == "__main__":
    start_consumer()