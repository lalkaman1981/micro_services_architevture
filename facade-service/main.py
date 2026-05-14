import os
import uuid
import random
import asyncio
import time
import logging
from datetime import datetime
from typing import Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("facade-service")

LOGGING_URLS = [
    u.strip() for u in os.getenv(
        "LOGGING_URLS",
        "http://logging-service-1:8001,http://logging-service-2:8001,http://logging-service-3:8001",
    ).split(",") if u.strip()
]
COUNTER_URL = os.getenv("COUNTER_URL", "http://counter-service:8002")

app = FastAPI(title="facade-service")

# Accumulated timing (seconds) for downstream calls
_logging_total: float = 0.0
_counter_total: float = 0.0
_logging_calls: int = 0
_counter_calls: int = 0


class TransactionIn(BaseModel):
    user_id: str
    amount: float


def _shuffled_logging_urls() -> list[str]:
    urls = LOGGING_URLS.copy()
    random.shuffle(urls)
    return urls


async def _logging_request(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    json: Optional[dict] = None,
) -> tuple[dict | list, float, str]:
    """
    Call ONE of the logging-service instances chosen randomly.
    If that instance is unreachable, fall through to the next, etc.
    Returns (response_json, elapsed_seconds, chosen_url).
    """
    last_err: Exception | None = None
    for url in _shuffled_logging_urls():
        target = f"{url}{path}"
        t = time.perf_counter()
        try:
            if method == "POST":
                r = await client.post(target, json=json, timeout=10.0)
            else:
                r = await client.get(target, timeout=10.0)
            r.raise_for_status()
            elapsed = time.perf_counter() - t
            log.info("logging call OK %s %s -> %s (%.4fs)", method, path, url, elapsed)
            return r.json(), elapsed, url
        except (httpx.HTTPError, httpx.RequestError, httpx.HTTPStatusError) as e:
            log.warning("logging call FAILED %s %s -> %s: %s; trying next", method, path, url, e)
            last_err = e
            continue
    raise HTTPException(
        status_code=502,
        detail=f"all logging-service instances unavailable (last error: {last_err})",
    )


async def _counter_post(client: httpx.AsyncClient, path: str, payload: dict):
    t = time.perf_counter()
    r = await client.post(f"{COUNTER_URL}{path}", json=payload, timeout=10.0)
    r.raise_for_status()
    return r.json(), time.perf_counter() - t


async def _counter_get(client: httpx.AsyncClient, path: str):
    t = time.perf_counter()
    r = await client.get(f"{COUNTER_URL}{path}", timeout=10.0)
    r.raise_for_status()
    return r.json(), time.perf_counter() - t


@app.post("/transactions")
async def post_transaction(tx: TransactionIn):
    global _logging_total, _counter_total, _logging_calls, _counter_calls

    transaction_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    payload = {
        "transaction_id": transaction_id,
        "user_id": tx.user_id,
        "amount": tx.amount,
    }

    log.info(
        "POST /transactions transaction_id=%s user_id=%s amount=%+.2f",
        transaction_id, tx.user_id, tx.amount,
    )

    async with httpx.AsyncClient() as client:
        try:
            (log_res, log_t, log_host), (cnt_res, cnt_t) = await asyncio.gather(
                _logging_request(client, "POST", "/log", json=payload),
                _counter_post(client, "/transaction", payload),
            )
        except HTTPException:
            raise
        except Exception as e:
            log.error("downstream error: %s", e)
            raise HTTPException(status_code=502, detail=str(e))

    _logging_total += log_t
    _counter_total += cnt_t
    _logging_calls += 1
    _counter_calls += 1

    balance = cnt_res["balance"]
    log.info(
        "DONE transaction_id=%s balance=%.2f log_host=%s log_t=%.4fs counter_t=%.4fs",
        transaction_id, balance, log_host, log_t, cnt_t,
    )
    return {
        "transaction_id": transaction_id,
        "balance": balance,
        "logging_instance": log_host,
    }


@app.get("/messages")
async def get_messages():
    """Return all transactions known to the logging-service cluster."""
    global _logging_total, _logging_calls
    async with httpx.AsyncClient() as client:
        msgs, log_t, log_host = await _logging_request(client, "GET", "/messages")
    _logging_total += log_t
    _logging_calls += 1
    return {"logging_instance": log_host, "messages": msgs}


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    global _logging_total, _counter_total, _logging_calls, _counter_calls

    async with httpx.AsyncClient() as client:
        try:
            (cnt_res, cnt_t), (log_res, log_t, log_host) = await asyncio.gather(
                _counter_get(client, f"/balance/{user_id}"),
                _logging_request(client, "GET", f"/user/{user_id}"),
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
            raise HTTPException(status_code=502, detail=str(e))
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))

    _logging_total += log_t
    _counter_total += cnt_t
    _logging_calls += 1
    _counter_calls += 1

    return {
        "balance": cnt_res["balance"],
        "transactions": log_res,
        "logging_instance": log_host,
    }


@app.get("/accounts")
async def get_accounts():
    global _counter_total, _counter_calls
    async with httpx.AsyncClient() as client:
        try:
            balances, cnt_t = await _counter_get(client, "/balances")
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))
    _counter_total += cnt_t
    _counter_calls += 1
    return balances


@app.get("/stats")
async def get_stats():
    return {
        "logging_service": {
            "total_time_s": round(_logging_total, 6),
            "call_count": _logging_calls,
            "avg_time_s": round(_logging_total / _logging_calls, 6) if _logging_calls else 0,
            "instances": LOGGING_URLS,
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


@app.get("/health")
async def health():
    return {"status": "ok", "logging_instances": LOGGING_URLS, "counter_url": COUNTER_URL}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
