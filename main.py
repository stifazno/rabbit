from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import pika
import json
import uuid
from datetime import datetime

from db import (
    init_db,
    insert_message,
    update_status,
    save_pending,
    get_all,
    get_stats,
    retry_message,
    get_service_status
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
        credentials = pika.PlainCredentials("guest", "guest")

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="rabbitmq", credentials=credentials)
        )

        channel = connection.channel()

        channel.queue_declare(queue="test_queue", durable=True)

        channel.basic_publish(
            exchange="",
            routing_key="test_queue",
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        connection.close()

    except Exception:
        save_pending(message["id"], message["text"])


@app.post("/send")
def send(data: dict):
    msg_id = str(uuid.uuid4())
    text = data.get("text")

    message = {"id": msg_id, "text": text}

    insert_message(msg_id, text, "Принято")
    send_message(message)

    return {"status": "sent", "message_id": msg_id}


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
        raise HTTPException(404, "Message not found")

    send_message(message)
    update_status(msg_id, "Повторная отправка")

    return {"status": "retried"}


@app.get("/consumer/messages")
def consumer_messages():
    conn = __import__("sqlite3").connect("messages.db")
    cur = conn.cursor()

    cur.execute("""
        SELECT text, created_at
        FROM messages
        ORDER BY created_at DESC
        LIMIT 50
    """)

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "text": r[0],
            "time": r[1]
        }
        for r in rows
    ]


@app.get("/status/consumer")
def consumer_status():
    heartbeat = get_service_status("consumer")

    if not heartbeat:
        return {"status": "offline"}

    try:
        last = datetime.strptime(heartbeat, "%Y-%m-%d %H:%M:%S")
    except:
        return {"status": "offline"}

    diff = datetime.now() - last

    return {
        "status": "online" if diff.total_seconds() < 10 else "offline"
    }


@app.get("/status/rabbitmq")
def rabbit_status():
    try:
        credentials = pika.PlainCredentials("guest", "guest")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="rabbitmq", credentials=credentials)
        )
        connection.close()

        return {"status": "online"}

    except Exception:
        return {"status": "offline"}