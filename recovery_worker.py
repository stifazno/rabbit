import time
import json
import pika

from db import get_pending, delete_pending


def send_to_rabbit(msg):
    credentials = pika.PlainCredentials('guest', 'guest')

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1', credentials=credentials)
    )

    channel = connection.channel()
    channel.queue_declare(queue='test_queue', durable=True)

    channel.basic_publish(
        exchange='',
        routing_key='test_queue',
        body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    connection.close()


while True:
    try:
        pending = get_pending()

        if pending:
            print(f"🔄 Restoring {len(pending)} messages...")

        for msg_id, text in pending:
            try:
                send_to_rabbit({"id": msg_id, "text": text})
                delete_pending(msg_id)

                print(f"✔ Restored: {msg_id}")

            except Exception:
                print("⚠ RabbitMQ still down")
                break

        time.sleep(5)

    except Exception as e:
        print("Worker error:", e)
        time.sleep(5)