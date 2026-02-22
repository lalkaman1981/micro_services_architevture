import uuid
import asyncio
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uvicorn
from datetime import datetime

app = FastAPI(title="facade-service")

LOGGING_URL = "http://logging-service:8001"
COUNTER_URL = "http://counter-service:8002"

# Accumulated timing (seconds) for downstream calls
_logging_total: float = 0.0
_counter_total: float = 0.0
_logging_calls: int = 0
_counter_calls: int = 0


class TransactionIn(BaseModel):
    user_id: str
    amount: float  # positive = credit, negative = debit


async def _timed_post(client: httpx.AsyncClient, url: str, payload: dict):
    t = time.perf_counter()
    r = await client.post(url, json=payload)
    r.raise_for_status()
    return r.json(), time.perf_counter() - t


async def _timed_get(client: httpx.AsyncClient, url: str):
    t = time.perf_counter()
    r = await client.get(url)
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

    print(f"[facade] {timestamp} POST /transactions transaction_id={transaction_id} user_id={tx.user_id} amount={tx.amount:+.2f}")

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            (log_res, log_t), (cnt_res, cnt_t) = await asyncio.gather(
                _timed_post(client, f"{LOGGING_URL}/log", payload),
                _timed_post(client, f"{COUNTER_URL}/transaction", payload),
            )
        except Exception as e:
            print(f"[facade] ERROR transaction_id={transaction_id}: {e}")
            raise HTTPException(status_code=502, detail=str(e))

    _logging_total += log_t
    _counter_total += cnt_t
    _logging_calls += 1
    _counter_calls += 1

    balance = cnt_res["balance"]
    print(f"[facade] {datetime.now().isoformat()} transaction_id={transaction_id} balance={balance:.2f} log_t={log_t:.4f}s counter_t={cnt_t:.4f}s")
    return {"transaction_id": transaction_id, "balance": balance}


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    global _logging_total, _counter_total, _logging_calls, _counter_calls

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            (cnt_res, cnt_t), (log_res, log_t) = await asyncio.gather(
                _timed_get(client, f"{COUNTER_URL}/balance/{user_id}"),
                _timed_get(client, f"{LOGGING_URL}/user/{user_id}"),
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
            raise HTTPException(status_code=502, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))

    _logging_total += log_t
    _counter_total += cnt_t
    _logging_calls += 1
    _counter_calls += 1

    return {"balance": cnt_res["balance"], "transactions": log_res}


@app.get("/accounts")
async def get_accounts():
    global _counter_total, _counter_calls

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            balances, cnt_t = await _timed_get(client, f"{COUNTER_URL}/balances")
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))

    _counter_total += cnt_t
    _counter_calls += 1

    return balances


@app.get("/stats")
async def get_stats():
    """Return accumulated timing for logging-service and counter-service calls."""
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
    """Reset accumulated timing counters."""
    global _logging_total, _counter_total, _logging_calls, _counter_calls
    _logging_total = _counter_total = 0.0
    _logging_calls = _counter_calls = 0
    return {"status": "reset"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
