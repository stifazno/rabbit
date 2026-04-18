import pika
import json
import time
import traceback
from db import update_status

QUEUE_NAME = "test_queue"


def log(msg):
    print(f"[CONSUMER] {msg}", flush=True)


def callback(ch, method, properties, body):
    log("📩 RAW MESSAGE RECEIVED")
    log(f"BODY: {body}")

    try:
        message = json.loads(body)
        msg_id = message["id"]

        log(f"📦 PARSED MESSAGE: {message}")
        log(f"🆔 MESSAGE ID: {msg_id}")

        log("🟡 updating status -> processing")
        update_status(msg_id, "processing")

        log("⏳ simulating work (2s)")
        time.sleep(2)

        if message.get("text") == "error":
            raise Exception("Test error triggered manually")

        log("🟢 updating status -> done")
        update_status(msg_id, "done")

        ch.basic_ack(delivery_tag=method.delivery_tag)
        log(f"✔ ACK sent for {msg_id}")

    except Exception as e:
        log("❌ ERROR IN CALLBACK")
        log(str(e))
        log(traceback.format_exc())

        try:
            update_status(msg_id, "failed")
        except Exception as db_err:
            log(f"⚠ DB update failed: {db_err}")

        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def connect():
    log("🔄 CONNECTING TO RABBITMQ...")

    credentials = pika.PlainCredentials("guest", "guest")

    params = pika.ConnectionParameters(
        host="rabbitmq",
        port=5672,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
        connection_attempts=10,
        retry_delay=3,
    )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    log("📡 CONNECTED")

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # ❗ важно: чистим старые состояния подписки
    try:
        channel.cancel()
    except:
        pass

    channel.basic_qos(prefetch_count=1)

    return connection, channel


def start_consumer():
    log("🚀 CONSUMER BOOTING...")

    while True:
        connection = None

        try:
            connection, channel = connect()

            # ❗ ВАЖНО: auto_ack=False
            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=callback,
                auto_ack=False
            )

            log("🎧 START CONSUMING")

            channel.start_consuming()

        except Exception as e:
            log("💥 CONNECTION ERROR / LOST CONNECTION")
            log(str(e))
            log(traceback.format_exc())

            try:
                if connection:
                    connection.close()
            except:
                pass

            log("🔁 retry in 3 seconds...")
            time.sleep(3)


if __name__ == "__main__":
    start_consumer()