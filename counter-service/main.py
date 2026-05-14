import os
import logging
from contextlib import asynccontextmanager

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("counter-service")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://counter:counter@postgres:5432/counter",
)


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float  # positive = credit, negative = debit


pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    log.info("connecting to PostgreSQL at %s", DATABASE_URL)
    last_err: Exception | None = None
    # postgres may still be starting; retry briefly
    import asyncio as _asyncio
    for attempt in range(30):
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
            break
        except Exception as e:
            last_err = e
            log.info("postgres not ready (attempt %d): %s", attempt + 1, e)
            await _asyncio.sleep(1.0)
    if pool is None:
        raise RuntimeError(f"could not connect to postgres: {last_err}")

    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS balances (
                user_id TEXT PRIMARY KEY,
                balance DOUBLE PRECISION NOT NULL DEFAULT 0
            )
            """
        )
    log.info("postgres ready, schema ensured")
    try:
        yield
    finally:
        if pool is not None:
            await pool.close()


app = FastAPI(title="counter-service", lifespan=lifespan)


@app.post("/transaction")
async def apply_transaction(tx: Transaction):
    assert pool is not None
    async with pool.acquire() as conn:
        new_bal = await conn.fetchval(
            """
            INSERT INTO balances (user_id, balance) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE
            SET balance = balances.balance + EXCLUDED.balance
            RETURNING balance
            """,
            tx.user_id, tx.amount,
        )
    log.info(
        "transaction_id=%s user_id=%s amount=%+.2f balance=%.2f",
        tx.transaction_id, tx.user_id, tx.amount, new_bal,
    )
    return {"user_id": tx.user_id, "balance": float(new_bal)}


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    assert pool is not None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance FROM balances WHERE user_id = $1", user_id
        )
    if row is None:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    bal = float(row["balance"])
    log.info("GET /balance/%s -> %.2f", user_id, bal)
    return {"user_id": user_id, "balance": bal}


@app.get("/balances")
async def get_all_balances():
    assert pool is not None
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, balance FROM balances")
    out = {r["user_id"]: float(r["balance"]) for r in rows}
    log.info("GET /balances -> %d accounts", len(out))
    return out


@app.get("/health")
async def health():
    assert pool is not None
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
