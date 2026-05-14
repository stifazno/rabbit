import pika
import json
import time
import traceback

from db import update_status

QUEUE_NAME = "test_queue"


def log(msg):
    print(f"[CONSUMER] {msg}", flush=True)


def callback(ch, method, properties, body):

    try:
        message = json.loads(body)

        msg_id = message["id"]

        log(f"Получено сообщение {msg_id}")

        update_status(msg_id, "В процессе")

        time.sleep(2)

        if message.get("text") == "error":
            raise Exception("Тестовая ошибка")

        update_status(msg_id, "Отправлено")

        ch.basic_ack(
            delivery_tag=method.delivery_tag
        )

        log(f"✔ Успешно обработано {msg_id}")

    except Exception as e:

        log(f"Ошибка {e}")

        log(traceback.format_exc())

        update_status(
            msg_id,
            "Ошибка отправки"
        )

        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False
        )


def start_consumer():

    while True:

        connection = None

        try:
            log("Подключение к RabbitMQ")

            credentials = pika.PlainCredentials(
                'guest',
                'guest'
            )

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='rabbitmq',
                    credentials=credentials
                )
            )

            channel = connection.channel()

            channel.queue_declare(
                queue=QUEUE_NAME,
                durable=True
            )

            channel.basic_qos(
                prefetch_count=1
            )

            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=callback,
                auto_ack=False
            )

            log("Ожидание сообщений")

            channel.start_consuming()

        except Exception as e:

            log(f"RabbitMQ недоступен: {e}")

            try:
                if connection:
                    connection.close()
            except:
                pass

            time.sleep(3)


if __name__ == "__main__":
    start_consumer()