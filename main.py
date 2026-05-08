from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pika
import json
import uuid

from db import (
    init_db,
    insert_message,
    update_status,
    save_pending,
    get_all,
    get_stats,
    retry_message
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()


def send_message(message):
    try:
        credentials = pika.PlainCredentials('guest', 'guest')

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='rabbitmq',
                credentials=credentials
            )
        )

        channel = connection.channel()

        channel.queue_declare(
            queue='test_queue',
            durable=True
        )

        channel.basic_publish(
            exchange='',
            routing_key='test_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

        connection.close()

        print("✔ Отправлено в RabbitMQ")

    except Exception:
        print("⚠ RabbitMQ недоступен → сохраняем локально")

        save_pending(message["id"], message["text"])


@app.post("/send")
def send(data: dict):
    try:
        msg_id = str(uuid.uuid4())
        text = data.get("text")

        message = {
            "id": msg_id,
            "text": text
        }

        insert_message(
            msg_id,
            text,
            "Принято"
        )

        send_message(message)

        return {
            "status": "sent",
            "message_id": msg_id
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/messages")
def messages():
    return get_all()


@app.get("/stats")
def stats():
    return get_stats()


@app.post("/retry/{msg_id}")
def retry(msg_id: str):

    message = retry_message(msg_id)

    if not message:
        raise HTTPException(
            status_code=404,
            detail="Message not found"
        )

    send_message(message)

    update_status(msg_id, "Повторная отправка")

    return {
        "status": "retried"
    }