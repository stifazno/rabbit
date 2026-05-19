from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import pika
import json
import uuid
import os
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

# FIX: нормальный путь внутри контейнера
app.mount("/static", StaticFiles(directory="/app"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()

clients = []


# ─────────────────────────────
# WEBSOCKET
# ─────────────────────────────

@app.websocket("/ws/consumer")
async def ws_consumer(websocket: WebSocket):
    print("🔥 WS CONNECT ATTEMPT")

    await websocket.accept()
    clients.append(websocket)

    print("✅ CLIENTS:", len(clients))

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        print("❌ WS DISCONNECT")

    finally:
        if websocket in clients:
            clients.remove(websocket)


async def push_to_clients(data: dict):
    dead = []

    for client in clients:
        try:
            await client.send_json(data)
        except:
            dead.append(client)

    for d in dead:
        if d in clients:
            clients.remove(d)


# ─────────────────────────────
# INTERNAL PUSH FROM CONSUMER
# ─────────────────────────────

@app.post("/internal/push")
async def internal_push(data: dict):
    print("📨 INTERNAL PUSH:", data)
    print("👥 CLIENTS:", len(clients))

    await push_to_clients(data)
    return {"ok": True}


# ─────────────────────────────
# HTML PAGE (FIX PATH)
# ─────────────────────────────

@app.get("/consumer.html")
def consumer_page():
    return FileResponse(
        os.path.join(os.path.dirname(__file__), "consumer.html")
    )


# ─────────────────────────────
# RABBIT HELPERS
# ─────────────────────────────

def get_connection():
    credentials = pika.PlainCredentials("guest", "guest")

    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host="rabbitmq",
            credentials=credentials
        )
    )


def send_to_consumer(message: dict):
    try:
        connection = get_connection()
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


# ─────────────────────────────
# API
# ─────────────────────────────

@app.post("/send")
def send(data: dict):
    msg_id = str(uuid.uuid4())
    text = data.get("text")

    message = {
        "id": msg_id,
        "text": text
    }

    insert_message(msg_id, text, "Отправлено в очередь")
    send_to_consumer(message)

    return {
        "status": "sent",
        "message_id": msg_id
    }


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

    send_to_consumer(message)
    update_status(msg_id, "Повторная отправка")

    return {"status": "retried"}


# ─────────────────────────────
# STATUS
# ─────────────────────────────

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
        conn = get_connection()
        conn.close()
        return {"status": "online"}
    except:
        return {"status": "offline"}
    

@app.get("/internal/history")
def internal_history():
    conn = __import__("sqlite3").connect("messages.db")
    cur = conn.cursor()

    cur.execute("""
        SELECT id, text, created_at, status
        FROM messages
        ORDER BY created_at DESC
        LIMIT 50
    """)

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "id": r[0],
            "text": r[1],
            "time": r[2],
            "status": r[3]
        }
        for r in rows
    ]