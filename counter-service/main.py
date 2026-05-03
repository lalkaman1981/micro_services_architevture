import os
import json
import asyncio
import threading
import time
import urllib.parse
import consul
import hazelcast
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
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
balances: Dict[str, float] = {}


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


def _kv_get(key: str) -> str:
    _, data = consul_client.kv.get(key)
    if not data:
        raise RuntimeError(f"Consul KV key not found: {key}")
    return data["Value"].decode()


def consume_queue(client: hazelcast.HazelcastClient, queue_name: str):
    queue = client.get_queue(queue_name).blocking()
    print("[counter] Queue consumer started")
    while True:
        try:
            item = queue.take()
            msg = json.loads(item)
            prev = balances.get(msg["user_id"], 0.0)
            balances[msg["user_id"]] = prev + msg["amount"]
            new_bal = balances[msg["user_id"]]
            print(
                f"[counter] {datetime.now().isoformat()} MQ "
                f"transaction_id={msg['transaction_id']} "
                f"user_id={msg['user_id']} amount={msg['amount']:+.2f} balance={new_bal:.2f}"
            )
        except Exception as e:
            print(f"[counter] Queue consumer error: {e}")
            time.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client

    mq_hosts = mq_cluster = mq_queue = None
    for attempt in range(15):
        try:
            mq_hosts = _kv_get("config/mq/hosts").split(",")
            mq_cluster = _kv_get("config/mq/cluster_name")
            mq_queue = _kv_get("config/mq/queue_name")
            print(f"[counter] MQ config from Consul KV: hosts={mq_hosts} cluster={mq_cluster} queue={mq_queue}")
            break
        except Exception as e:
            print(f"[counter] Consul KV attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

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


app = FastAPI(title="counter-service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    if user_id not in balances:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    bal = balances[user_id]
    print(f"[counter] GET /balance/{user_id} -> {bal:.2f}")
    return {"user_id": user_id, "balance": bal}


@app.get("/balances")
async def get_all_balances():
    print(f"[counter] GET /balances -> {len(balances)} accounts")
    return balances


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
