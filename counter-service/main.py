import os
import json
import asyncio
import threading
import time
import hazelcast
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
from contextlib import asynccontextmanager
from datetime import datetime

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8080")
SERVICE_URL = os.getenv("SERVICE_URL", "http://counter-service:8002")
HZ_HOSTS = os.getenv("HZ_HOSTS", "hazelcast-1:5701").split(",")
QUEUE_NAME = "counter-transactions"

hz_client = None
# In-memory store: user_id -> balance
balances: Dict[str, float] = {}


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


def consume_queue(client: hazelcast.HazelcastClient):
    queue = client.get_queue(QUEUE_NAME).blocking()
    print("[counter] Queue consumer started")
    while True:
        try:
            item = queue.take()  # blocks until an item is available
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
    global hz_client

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


app = FastAPI(title="counter-service", lifespan=lifespan)


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
