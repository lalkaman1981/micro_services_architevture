import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime

import hazelcast
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("logging-service")

INSTANCE_NAME = os.getenv("INSTANCE_NAME", "logging-service")
CLUSTER_NAME = os.getenv("HZ_CLUSTER_NAME", "dev-cluster")
PRIMARY_MEMBER = os.getenv("HZ_PRIMARY_MEMBER", "hazelcast-1:5701")
ALL_MEMBERS = os.getenv(
    "HZ_MEMBERS",
    "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
)
MAP_NAME = os.getenv("HZ_MAP_NAME", "messages")

# Ordered list: primary first (the node "owned" by this instance), then the rest as failover
members_list = [m.strip() for m in ALL_MEMBERS.split(",") if m.strip()]
if PRIMARY_MEMBER in members_list:
    members_list.remove(PRIMARY_MEMBER)
members_list = [PRIMARY_MEMBER] + members_list


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


hz_client = None
messages_map = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, messages_map
    log.info(
        "[%s] connecting to Hazelcast cluster '%s' via members=%s",
        INSTANCE_NAME, CLUSTER_NAME, members_list,
    )
    # smart_routing=False (unisocket mode): the client connects to exactly ONE
    # member at a time — its assigned HZ node — matching the task requirement
    # "logging-service connects to its own node of the Hazelcast cluster".
    # If that node is down, the client falls back through the rest of cluster_members.
    hz_client = hazelcast.HazelcastClient(
        cluster_name=CLUSTER_NAME,
        cluster_members=members_list,
        client_name=INSTANCE_NAME,
        async_start=False,
        reconnect_mode="ON",
        cluster_connect_timeout=60.0,
        retry_initial_backoff=1.0,
        retry_max_backoff=5.0,
        # Unisocket mode: connect to exactly ONE member at a time — this instance's
        # "own" HZ node — and fall over to the next in cluster_members if it dies.
        # shuffle_member_list=False preserves our ordering (primary first).
        smart_routing=False,
        shuffle_member_list=False,
    )
    messages_map = hz_client.get_map(MAP_NAME).blocking()
    log.info("[%s] connected; using map '%s'", INSTANCE_NAME, MAP_NAME)
    try:
        yield
    finally:
        log.info("[%s] shutting down Hazelcast client", INSTANCE_NAME)
        if hz_client is not None:
            hz_client.shutdown()


app = FastAPI(title="logging-service", lifespan=lifespan)


@app.post("/log")
def receive_log(tx: Transaction):
    entry = tx.model_dump()
    # put_if_absent gives idempotent dedup by transaction_id
    prev = messages_map.put_if_absent(tx.transaction_id, entry)
    if prev is not None:
        log.info(
            "[%s] DUPLICATE transaction_id=%s user_id=%s amount=%+.2f",
            INSTANCE_NAME, tx.transaction_id, tx.user_id, tx.amount,
        )
        return {"status": "duplicate", "transaction_id": tx.transaction_id, "instance": INSTANCE_NAME}
    log.info(
        "[%s] STORED transaction_id=%s user_id=%s amount=%+.2f map_size=%d",
        INSTANCE_NAME, tx.transaction_id, tx.user_id, tx.amount, messages_map.size(),
    )
    return {"status": "stored", "transaction_id": tx.transaction_id, "instance": INSTANCE_NAME}


@app.get("/messages")
def get_all_transactions():
    try:
        values = messages_map.values()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"hazelcast unavailable: {e}")
    result = list(values)
    log.info("[%s] GET /messages -> %d transactions", INSTANCE_NAME, len(result))
    return result


@app.get("/user/{user_id}")
def get_user_transactions(user_id: str):
    try:
        values = messages_map.values()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"hazelcast unavailable: {e}")
    txs = [v for v in values if v.get("user_id") == user_id]
    log.info("[%s] GET /user/%s -> %d transactions", INSTANCE_NAME, user_id, len(txs))
    return txs


@app.get("/health")
def health():
    try:
        size = messages_map.size()
        return {
            "status": "ok",
            "instance": INSTANCE_NAME,
            "primary_member": PRIMARY_MEMBER,
            "map_size": size,
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"hazelcast unavailable: {e}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
