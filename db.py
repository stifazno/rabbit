import sqlite3

DB_NAME = "messages.db"


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        text TEXT,
        status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending_messages (
        id TEXT PRIMARY KEY,
        text TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS service_status (
        service TEXT PRIMARY KEY,
        last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()


def insert_message(msg_id, text, status):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO messages (id, text, status) VALUES (?, ?, ?)",
        (msg_id, text, status)
    )

    conn.commit()
    conn.close()


def update_status(msg_id, status):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute(
        "UPDATE messages SET status=? WHERE id=?",
        (status, msg_id)
    )

    conn.commit()
    conn.close()


def get_all():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM messages
        ORDER BY created_at DESC
    """)

    rows = cur.fetchall()

    conn.close()
    return rows


def save_pending(msg_id, text):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute(
        "INSERT OR REPLACE INTO pending_messages VALUES (?, ?)",
        (msg_id, text)
    )

    conn.commit()
    conn.close()


def get_pending():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT * FROM pending_messages")
    rows = cur.fetchall()

    conn.close()
    return rows


def delete_pending(msg_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute(
        "DELETE FROM pending_messages WHERE id=?",
        (msg_id,)
    )

    conn.commit()
    conn.close()


def get_stats():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM messages")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM messages WHERE status='Отправлено'")
    done = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM messages WHERE status='Ошибка отправки'")
    failed = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM messages WHERE status='В процессе'")
    processing = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM messages WHERE status='Принято'")
    sent = cur.fetchone()[0]

    conn.close()

    return {
        "total": total,
        "done": done,
        "failed": failed,
        "processing": processing,
        "sent": sent
    }


def retry_message(msg_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute(
        "SELECT id, text FROM messages WHERE id=?",
        (msg_id,)
    )

    row = cur.fetchone()
    conn.close()

    if row:
        return {"id": row[0], "text": row[1]}

    return None


def get_consumer_messages():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        SELECT text, status, created_at
        FROM messages
        ORDER BY created_at ASC
    """)

    rows = cur.fetchall()
    conn.close()

    return rows


# ✅ HEARTBEAT (ОДИН ИСТОЧНИК ПРАВДЫ)
def update_heartbeat(service: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO service_status(service, last_heartbeat)
        VALUES(?, CURRENT_TIMESTAMP)
        ON CONFLICT(service)
        DO UPDATE SET last_heartbeat=CURRENT_TIMESTAMP
    """, (service,))

    conn.commit()
    conn.close()


def get_service_status(service: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        SELECT last_heartbeat FROM service_status
        WHERE service=?
    """, (service,))

    row = cur.fetchone()
    conn.close()

    return row[0] if row else None