import os
import json
import sqlite3
import asyncio
import threading
import time
import hazelcast
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from datetime import datetime

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8080")
SERVICE_URL = os.getenv("SERVICE_URL", "http://counter-service:8002")
HZ_HOSTS = os.getenv("HZ_HOSTS", "hazelcast-1:5701").split(",")
QUEUE_NAME = "counter-transactions"
DB_PATH = os.getenv("DB_PATH", "/data/counter.db")
# Simulated extra write latency in seconds to model "slow disk DB" per task wording.
WRITE_DELAY_S = float(os.getenv("WRITE_DELAY_S", "0.5"))

hz_client = None
# A single SQLite connection is fine here — only the consumer thread writes,
# and the FastAPI read endpoints open short-lived read connections.
_db_lock = threading.Lock()
_db: sqlite3.Connection | None = None


def init_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS balances (
            user_id TEXT PRIMARY KEY,
            balance REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS applied_transactions (
            transaction_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            amount REAL NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )
    return conn


def apply_transaction(tx: dict) -> float | None:
    """Idempotently apply a transaction to the on-disk DB. Returns new balance, or None if duplicate."""
    if WRITE_DELAY_S > 0:
        # Simulate slow disk DB write so the MQ buffering value is observable.
        time.sleep(WRITE_DELAY_S)
    with _db_lock:
        cur = _db.execute(
            "SELECT 1 FROM applied_transactions WHERE transaction_id = ?",
            (tx["transaction_id"],),
        )
        if cur.fetchone() is not None:
            return None
        _db.execute("BEGIN")
        try:
            _db.execute(
                "INSERT INTO balances(user_id, balance) VALUES(?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET balance = balance + excluded.balance",
                (tx["user_id"], tx["amount"]),
            )
            _db.execute(
                "INSERT INTO applied_transactions(transaction_id, user_id, amount, applied_at) "
                "VALUES(?, ?, ?, ?)",
                (tx["transaction_id"], tx["user_id"], tx["amount"], datetime.now().isoformat()),
            )
            _db.execute("COMMIT")
        except Exception:
            _db.execute("ROLLBACK")
            raise
        row = _db.execute(
            "SELECT balance FROM balances WHERE user_id = ?", (tx["user_id"],)
        ).fetchone()
        return float(row[0]) if row else 0.0


def consume_queue(client: hazelcast.HazelcastClient):
    queue = client.get_queue(QUEUE_NAME).blocking()
    print("[counter] Queue consumer started")
    while True:
        try:
            item = queue.take()  # blocks until an item is available
            msg = json.loads(item)
            new_bal = apply_transaction(msg)
            if new_bal is None:
                print(
                    f"[counter] {datetime.now().isoformat()} MQ DUPLICATE "
                    f"transaction_id={msg['transaction_id']}"
                )
            else:
                print(
                    f"[counter] {datetime.now().isoformat()} MQ APPLIED "
                    f"transaction_id={msg['transaction_id']} "
                    f"user_id={msg['user_id']} amount={msg['amount']:+.2f} "
                    f"balance={new_bal:.2f}"
                )
        except Exception as e:
            print(f"[counter] Queue consumer error: {e}")
            time.sleep(1)


async def register_with_config_server():
    for attempt in range(10):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.post(
                    f"{CONFIG_SERVER_URL}/register",
                    json={"service_name": "counter-service", "url": SERVICE_URL},
                )
                r.raise_for_status()
                print(f"[counter] Registered at config-server: {SERVICE_URL}")
                return
        except Exception as e:
            print(f"[counter] Registration attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, _db

    _db = init_db()
    print(f"[counter] SQLite DB ready at {DB_PATH}")

    for attempt in range(10):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=HZ_HOSTS,
                cluster_name="dev",
            )
            print(f"[counter] Connected to Hazelcast at {HZ_HOSTS}")
            break
        except Exception as e:
            print(f"[counter] Hazelcast connect attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

    consumer_thread = threading.Thread(target=consume_queue, args=(hz_client,), daemon=True)
    consumer_thread.start()

    await register_with_config_server()

    yield

    if hz_client:
        hz_client.shutdown()
    if _db:
        _db.close()


app = FastAPI(title="counter-service", lifespan=lifespan)


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    with _db_lock:
        row = _db.execute(
            "SELECT balance FROM balances WHERE user_id = ?", (user_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    bal = float(row[0])
    print(f"[counter] GET /balance/{user_id} -> {bal:.2f}")
    return {"user_id": user_id, "balance": bal}


@app.get("/balances")
async def get_all_balances():
    with _db_lock:
        rows = _db.execute("SELECT user_id, balance FROM balances").fetchall()
    result = {uid: float(bal) for uid, bal in rows}
    print(f"[counter] GET /balances -> {len(result)} accounts")
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
