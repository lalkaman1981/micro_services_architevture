import os
import json
import asyncio
import threading
import time
import urllib.parse
import consul
import hazelcast
import psycopg2
from psycopg2 import pool as pg_pool
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
SERVICE_URL = os.getenv("SERVICE_URL", "http://counter-service:8002")

_parsed = urllib.parse.urlparse(SERVICE_URL)
SERVICE_HOST = _parsed.hostname
SERVICE_PORT = _parsed.port or 8002
SERVICE_ID = f"counter-service-{SERVICE_HOST}"

consul_client = consul.Consul(host=CONSUL_HOST, port=8500)

hz_client = None
db_pool: pg_pool.ThreadedConnectionPool = None


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


def _kv_get(key: str) -> str:
    _, data = consul_client.kv.get(key)
    if not data:
        raise RuntimeError(f"Consul KV key not found: {key}")
    return data["Value"].decode()


def _init_db_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS balances (
                user_id   TEXT PRIMARY KEY,
                balance   NUMERIC NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_transactions (
                transaction_id TEXT PRIMARY KEY,
                user_id        TEXT NOT NULL,
                amount         NUMERIC NOT NULL,
                processed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    conn.commit()


def _apply_transaction(msg: dict) -> float:
    """Apply a transaction to the DB atomically; return new balance."""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_transactions (transaction_id, user_id, amount) "
                "VALUES (%s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING",
                (msg["transaction_id"], msg["user_id"], msg["amount"]),
            )
            if cur.rowcount == 0:
                cur.execute("SELECT balance FROM balances WHERE user_id = %s", (msg["user_id"],))
                row = cur.fetchone()
                conn.commit()
                return float(row[0]) if row else 0.0

            cur.execute(
                "INSERT INTO balances (user_id, balance) VALUES (%s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET balance = balances.balance + EXCLUDED.balance "
                "RETURNING balance",
                (msg["user_id"], msg["amount"]),
            )
            new_balance = float(cur.fetchone()[0])
        conn.commit()
        return new_balance
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)


def _get_balance(user_id: str):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM balances WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
        return float(row[0]) if row else None
    finally:
        db_pool.putconn(conn)


def _get_all_balances() -> dict:
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, balance FROM balances")
            rows = cur.fetchall()
        return {uid: float(bal) for uid, bal in rows}
    finally:
        db_pool.putconn(conn)


def consume_queue(client: hazelcast.HazelcastClient, queue_name: str):
    queue = client.get_queue(queue_name).blocking()
    print("[counter] Queue consumer started")
    while True:
        try:
            item = queue.take()
            msg = json.loads(item)
            new_bal = _apply_transaction(msg)
            print(
                f"[counter] {datetime.now().isoformat()} MQ "
                f"transaction_id={msg['transaction_id']} "
                f"user_id={msg['user_id']} amount={msg['amount']:+.2f} balance={new_bal:.2f}"
            )
        except Exception as e:
            print(f"[counter] Queue consumer error: {e}")
            time.sleep(1)


def _connect_db(host: str, port: int, user: str, password: str, dbname: str):
    last_err = None
    for attempt in range(30):
        try:
            p = pg_pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=dbname,
            )
            test = p.getconn()
            _init_db_schema(test)
            p.putconn(test)
            print(f"[counter] Connected to Postgres at {host}:{port}/{dbname}")
            return p
        except Exception as e:
            last_err = e
            print(f"[counter] Postgres connect attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Postgres: {last_err}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, db_pool

    mq_hosts = mq_cluster = mq_queue = None
    db_host = db_port = db_user = db_pass = db_name = None
    for attempt in range(15):
        try:
            mq_hosts = _kv_get("config/mq/hosts").split(",")
            mq_cluster = _kv_get("config/mq/cluster_name")
            mq_queue = _kv_get("config/mq/queue_name")
            db_host = _kv_get("config/db/host")
            db_port = int(_kv_get("config/db/port"))
            db_user = _kv_get("config/db/user")
            db_pass = _kv_get("config/db/password")
            db_name = _kv_get("config/db/name")
            print(
                f"[counter] MQ config from Consul KV: hosts={mq_hosts} cluster={mq_cluster} queue={mq_queue}"
            )
            print(f"[counter] DB config from Consul KV: {db_user}@{db_host}:{db_port}/{db_name}")
            break
        except Exception as e:
            print(f"[counter] Consul KV attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

    db_pool = await asyncio.to_thread(
        _connect_db, db_host, db_port, db_user, db_pass, db_name
    )

    for attempt in range(10):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=mq_hosts,
                cluster_name=mq_cluster,
            )
            print(f"[counter] Connected to Hazelcast at {mq_hosts}")
            break
        except Exception as e:
            print(f"[counter] Hazelcast connect attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

    consumer_thread = threading.Thread(
        target=consume_queue, args=(hz_client, mq_queue), daemon=True
    )
    consumer_thread.start()

    for attempt in range(10):
        try:
            consul_client.agent.service.register(
                name="counter-service",
                service_id=SERVICE_ID,
                address=SERVICE_HOST,
                port=SERVICE_PORT,
                check=consul.Check.http(
                    f"http://{SERVICE_HOST}:{SERVICE_PORT}/health",
                    interval="10s",
                    timeout="5s",
                    deregister="30s",
                ),
            )
            print(f"[counter] Registered with Consul: {SERVICE_ID} at {SERVICE_HOST}:{SERVICE_PORT}")
            break
        except Exception as e:
            print(f"[counter] Consul registration attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(2)

    yield

    try:
        consul_client.agent.service.deregister(SERVICE_ID)
    except Exception:
        pass
    if hz_client:
        hz_client.shutdown()
    if db_pool:
        db_pool.closeall()


app = FastAPI(title="counter-service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    bal = await asyncio.to_thread(_get_balance, user_id)
    if bal is None:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    print(f"[counter] GET /balance/{user_id} -> {bal:.2f}")
    return {"user_id": user_id, "balance": bal}


@app.get("/balances")
async def get_all_balances():
    balances = await asyncio.to_thread(_get_all_balances)
    print(f"[counter] GET /balances -> {len(balances)} accounts")
    return balances


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
