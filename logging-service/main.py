import os
import json
import asyncio
import urllib.parse
import consul
import hazelcast
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
SERVICE_URL = os.getenv("SERVICE_URL", "http://logging-service:8001")

_parsed = urllib.parse.urlparse(SERVICE_URL)
SERVICE_HOST = _parsed.hostname
SERVICE_PORT = _parsed.port or 8001
SERVICE_ID = f"logging-service-{SERVICE_HOST}"

consul_client = consul.Consul(host=CONSUL_HOST, port=8500)

hz_client = None
msg_map = None


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


def _kv_get(key: str) -> str:
    _, data = consul_client.kv.get(key)
    if not data:
        raise RuntimeError(f"Consul KV key not found: {key}")
    return data["Value"].decode()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, msg_map

    hz_hosts = hz_cluster = hz_map = None
    for attempt in range(15):
        try:
            hz_hosts = _kv_get("config/hazelcast/hosts").split(",")
            hz_cluster = _kv_get("config/hazelcast/cluster_name")
            hz_map = _kv_get("config/hazelcast/map_name")
            print(f"[logging] Hazelcast config from Consul KV: hosts={hz_hosts} cluster={hz_cluster} map={hz_map}")
            break
        except Exception as e:
            print(f"[logging] Consul KV attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

    for attempt in range(10):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=hz_hosts,
                cluster_name=hz_cluster,
            )
            msg_map = hz_client.get_map(hz_map).blocking()
            print(f"[logging] Connected to Hazelcast at {hz_hosts}")
            break
        except Exception as e:
            print(f"[logging] Hazelcast connect attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(3)

    for attempt in range(10):
        try:
            consul_client.agent.service.register(
                name="logging-service",
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
            print(f"[logging] Registered with Consul: {SERVICE_ID} at {SERVICE_HOST}:{SERVICE_PORT}")
            break
        except Exception as e:
            print(f"[logging] Consul registration attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(2)

    yield

    try:
        consul_client.agent.service.deregister(SERVICE_ID)
    except Exception:
        pass
    if hz_client:
        hz_client.shutdown()


app = FastAPI(title="logging-service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


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
