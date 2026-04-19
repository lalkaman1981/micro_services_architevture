import os
import json
import uuid
import random
import asyncio
import time
import hazelcast
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8080")
SERVICE_URL = os.getenv("SERVICE_URL", "http://facade-service:8000")
HZ_HOSTS = os.getenv("HZ_HOSTS", "hazelcast-1:5701").split(",")
QUEUE_NAME = "counter-transactions"

hz_client = None
counter_queue = None

_logging_total: float = 0.0
_counter_total: float = 0.0
_logging_calls: int = 0
_counter_calls: int = 0


class TransactionIn(BaseModel):
    user_id: str
    amount: float  # positive = credit, negative = debit


async def get_service_urls(service_name: str):
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.get(f"{CONFIG_SERVER_URL}/services/{service_name}")
        r.raise_for_status()
        return r.json()["urls"]


async def get_random_url(service_name: str) -> str:
    urls = await get_service_urls(service_name)
    if not urls:
        raise HTTPException(status_code=503, detail=f"No instances of {service_name} registered")
    chosen = random.choice(urls)
    print(f"[facade] Selected {service_name} instance: {chosen}")
    return chosen


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, counter_queue

    for attempt in range(10):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=HZ_HOSTS,
                cluster_name="dev",
            )
            counter_queue = hz_client.get_queue(QUEUE_NAME).blocking()
            print(f"[facade] Connected to Hazelcast at {HZ_HOSTS}")
            break
        except Exception as e:
            print(f"[facade] Hazelcast connect attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

    for attempt in range(10):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{CONFIG_SERVER_URL}/register",
                    json={"service_name": "facade-service", "url": SERVICE_URL},
                )
            print(f"[facade] Registered at config-server: {SERVICE_URL}")
            break
        except Exception as e:
            print(f"[facade] Registration attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(2)

    yield

    if hz_client:
        hz_client.shutdown()


app = FastAPI(title="facade-service", lifespan=lifespan)


@app.post("/transactions")
async def post_transaction(tx: TransactionIn):
    global _logging_total, _logging_calls

    transaction_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    payload = {
        "transaction_id": transaction_id,
        "user_id": tx.user_id,
        "amount": tx.amount,
    }

    print(f"[facade] {timestamp} POST /transactions transaction_id={transaction_id} user_id={tx.user_id} amount={tx.amount:+.2f}")

    # Forward to a randomly chosen logging-service instance
    logging_url = await get_random_url("logging-service")
    t = time.perf_counter()
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.post(f"{logging_url}/log", json=payload)
            r.raise_for_status()
            log_res = r.json()
        except Exception as e:
            print(f"[facade] ERROR calling logging-service: {e}")
            raise HTTPException(status_code=502, detail=str(e))
    log_t = time.perf_counter() - t
    _logging_total += log_t
    _logging_calls += 1

    # Enqueue to Hazelcast for counter-service (fire-and-forget, no wait)
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, counter_queue.put, json.dumps(payload))
        print(f"[facade] {datetime.now().isoformat()} transaction_id={transaction_id} enqueued to counter-service MQ")
    except Exception as e:
        print(f"[facade] ERROR enqueuing to MQ: {e}")

    return {"transaction_id": transaction_id, "status": "queued", "logged": log_res}


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    global _logging_total, _counter_total, _logging_calls, _counter_calls

    balance = None
    transactions: list = []

    # Counter-service may be down (queue keeps absorbing writes meanwhile) — return null balance per task spec.
    try:
        counter_url = await get_random_url("counter-service")
        async with httpx.AsyncClient(timeout=3.0) as client:
            t_cnt = time.perf_counter()
            cnt_r = await client.get(f"{counter_url}/balance/{user_id}")
            _counter_total += time.perf_counter() - t_cnt
            _counter_calls += 1
        if cnt_r.status_code == 200:
            balance = cnt_r.json().get("balance")
    except Exception as e:
        print(f"[facade] WARN counter-service unreachable for /user/{user_id}: {e}")

    try:
        logging_url = await get_random_url("logging-service")
        async with httpx.AsyncClient(timeout=10.0) as client:
            t_log = time.perf_counter()
            log_r = await client.get(f"{logging_url}/user/{user_id}")
            _logging_total += time.perf_counter() - t_log
            _logging_calls += 1
        if log_r.status_code == 200:
            transactions = log_r.json()
    except Exception as e:
        print(f"[facade] WARN logging-service unreachable for /user/{user_id}: {e}")

    return {"balance": balance, "transactions": transactions}


@app.get("/accounts")
async def get_accounts():
    global _counter_total, _counter_calls

    try:
        counter_url = await get_random_url("counter-service")
    except HTTPException:
        return None

    t = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(f"{counter_url}/balances")
            r.raise_for_status()
    except Exception as e:
        print(f"[facade] WARN counter-service unreachable for /accounts: {e}")
        return None
    _counter_total += time.perf_counter() - t
    _counter_calls += 1

    return r.json()


@app.get("/stats")
async def get_stats():
    return {
        "logging_service": {
            "total_time_s": round(_logging_total, 6),
            "call_count": _logging_calls,
            "avg_time_s": round(_logging_total / _logging_calls, 6) if _logging_calls else 0,
        },
        "counter_service": {
            "total_time_s": round(_counter_total, 6),
            "call_count": _counter_calls,
            "avg_time_s": round(_counter_total / _counter_calls, 6) if _counter_calls else 0,
        },
    }


@app.post("/stats/reset")
async def reset_stats():
    global _logging_total, _counter_total, _logging_calls, _counter_calls
    _logging_total = _counter_total = 0.0
    _logging_calls = _counter_calls = 0
    return {"status": "reset"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
