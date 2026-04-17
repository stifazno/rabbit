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
    get_all
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()


# 🔥 отправка (RabbitMQ + fallback)
def send_message(message):
    try:
        credentials = pika.PlainCredentials('guest', 'guest')

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1', credentials=credentials)
        )

        channel = connection.channel()
        channel.queue_declare(queue='test_queue', durable=True)

        channel.basic_publish(
            exchange='',
            routing_key='test_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        connection.close()

        print("✔ Sent to RabbitMQ")

    except Exception:
        print("⚠ RabbitMQ DOWN → saving to SQLite")

        save_pending(message["id"], message["text"])


@app.post("/send")
def send(data: dict):
    try:
        msg_id = str(uuid.uuid4())
        text = data.get("text")

        message = {"id": msg_id, "text": text}

        insert_message(msg_id, text, "sent")

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