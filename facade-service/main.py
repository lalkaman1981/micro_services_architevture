import os
import json
import uuid
import random
import asyncio
import time
import urllib.parse
import consul
import hazelcast
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
SERVICE_URL = os.getenv("SERVICE_URL", "http://facade-service:8000")

_parsed = urllib.parse.urlparse(SERVICE_URL)
SERVICE_HOST = _parsed.hostname
SERVICE_PORT = _parsed.port or 8000
SERVICE_ID = f"facade-service-{SERVICE_HOST}"

consul_client = consul.Consul(host=CONSUL_HOST, port=8500)

hz_client = None
counter_queue = None

_logging_total: float = 0.0
_counter_total: float = 0.0
_logging_calls: int = 0
_counter_calls: int = 0


class TransactionIn(BaseModel):
    user_id: str
    amount: float


def _kv_get(key: str) -> str:
    _, data = consul_client.kv.get(key)
    if not data:
        raise RuntimeError(f"Consul KV key not found: {key}")
    return data["Value"].decode()


def _healthy_urls(service_name: str) -> list:
    _, services = consul_client.health.service(service_name, passing=True)
    return [
        f"http://{s['Service']['Address']}:{s['Service']['Port']}"
        for s in services
    ]


async def get_random_url(service_name: str) -> str:
    urls = await asyncio.to_thread(_healthy_urls, service_name)
    if not urls:
        raise HTTPException(status_code=503, detail=f"No healthy instances of {service_name}")
    chosen = random.choice(urls)
    print(f"[facade] Selected {service_name} instance: {chosen}")
    return chosen


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, counter_queue

    mq_hosts = mq_cluster = mq_queue = None
    for attempt in range(15):
        try:
            mq_hosts = _kv_get("config/mq/hosts").split(",")
            mq_cluster = _kv_get("config/mq/cluster_name")
            mq_queue = _kv_get("config/mq/queue_name")
            print(f"[facade] MQ config from Consul KV: hosts={mq_hosts} cluster={mq_cluster} queue={mq_queue}")
            break
        except Exception as e:
            print(f"[facade] Consul KV attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

    for attempt in range(10):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=mq_hosts,
                cluster_name=mq_cluster,
            )
            counter_queue = hz_client.get_queue(mq_queue).blocking()
            print(f"[facade] Connected to Hazelcast MQ at {mq_hosts}")
            break
        except Exception as e:
            print(f"[facade] Hazelcast connect attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

    for attempt in range(10):
        try:
            consul_client.agent.service.register(
                name="facade-service",
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
            print(f"[facade] Registered with Consul: {SERVICE_ID} at {SERVICE_HOST}:{SERVICE_PORT}")
            break
        except Exception as e:
            print(f"[facade] Consul registration attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(2)

    yield

    try:
        consul_client.agent.service.deregister(SERVICE_ID)
    except Exception:
        pass
    if hz_client:
        hz_client.shutdown()


app = FastAPI(title="facade-service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


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
    _logging_total += time.perf_counter() - t
    _logging_calls += 1

    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, counter_queue.put, json.dumps(payload))
        print(f"[facade] {datetime.now().isoformat()} transaction_id={transaction_id} enqueued to MQ")
    except Exception as e:
        print(f"[facade] ERROR enqueuing to MQ: {e}")

    return {"transaction_id": transaction_id, "status": "queued", "logged": log_res}


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    global _logging_total, _counter_total, _logging_calls, _counter_calls

    counter_url = await get_random_url("counter-service")
    logging_url = await get_random_url("logging-service")

    async with httpx.AsyncClient(timeout=10.0) as client:
        t_cnt = time.perf_counter()
        cnt_r = await client.get(f"{counter_url}/balance/{user_id}")
        _counter_total += time.perf_counter() - t_cnt
        _counter_calls += 1

        t_log = time.perf_counter()
        log_r = await client.get(f"{logging_url}/user/{user_id}")
        _logging_total += time.perf_counter() - t_log
        _logging_calls += 1

    balance = cnt_r.json().get("balance") if cnt_r.status_code == 200 else None
    transactions = log_r.json() if log_r.status_code == 200 else []

    return {"balance": balance, "transactions": transactions}


@app.get("/accounts")
async def get_accounts():
    global _counter_total, _counter_calls

    counter_url = await get_random_url("counter-service")
    t = time.perf_counter()
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.get(f"{counter_url}/balances")
            r.raise_for_status()
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))
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
