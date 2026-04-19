import os
import json
import asyncio
import hazelcast
import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8080")
SERVICE_URL = os.getenv("SERVICE_URL", "http://logging-service:8001")
HZ_HOSTS = os.getenv("HZ_HOSTS", "hazelcast-1:5701").split(",")
MAP_NAME = "logging-messages"

hz_client = None
msg_map = None


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


async def register_with_config_server():
    for attempt in range(10):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.post(
                    f"{CONFIG_SERVER_URL}/register",
                    json={"service_name": "logging-service", "url": SERVICE_URL},
                )
                r.raise_for_status()
                print(f"[logging] Registered at config-server: {SERVICE_URL}")
                return
        except Exception as e:
            print(f"[logging] Registration attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, msg_map

    for attempt in range(10):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=HZ_HOSTS,
                cluster_name="dev",
            )
            msg_map = hz_client.get_map(MAP_NAME).blocking()
            print(f"[logging] Connected to Hazelcast at {HZ_HOSTS}")
            break
        except Exception as e:
            print(f"[logging] Hazelcast connect attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

    await register_with_config_server()

    yield

    if hz_client:
        hz_client.shutdown()


app = FastAPI(title="logging-service", lifespan=lifespan)


@app.post("/log")
async def receive_log(tx: Transaction):
    if msg_map.contains_key(tx.transaction_id):
        print(f"[logging] {datetime.now().isoformat()} DUPLICATE transaction_id={tx.transaction_id} instance={SERVICE_URL}")
        return {"status": "duplicate", "transaction_id": tx.transaction_id}
    entry = tx.model_dump()
    msg_map.put(tx.transaction_id, json.dumps(entry))
    print(f"[logging] {datetime.now().isoformat()} STORED transaction_id={tx.transaction_id} user_id={tx.user_id} amount={tx.amount:+.2f} instance={SERVICE_URL}")
    return {"status": "stored", "transaction_id": tx.transaction_id}


@app.get("/messages")
async def get_all_transactions():
    values = msg_map.values()
    result = [json.loads(v) for v in values]
    print(f"[logging] GET /messages -> {len(result)} transactions instance={SERVICE_URL}")
    return result


@app.get("/user/{user_id}")
async def get_user_transactions(user_id: str):
    values = msg_map.values()
    txs = [json.loads(v) for v in values if json.loads(v)["user_id"] == user_id]
    print(f"[logging] GET /user/{user_id} -> {len(txs)} transactions instance={SERVICE_URL}")
    return txs


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
