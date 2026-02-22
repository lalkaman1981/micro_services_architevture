from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, List
import uvicorn
from datetime import datetime

app = FastAPI(title="logging-service")


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


# In-memory store: transaction_id -> transaction dict
store: Dict[str, dict] = {}


@app.post("/log")
async def receive_log(tx: Transaction):
    if tx.transaction_id in store:
        print(f"[logging] {datetime.now().isoformat()} DUPLICATE transaction_id={tx.transaction_id}")
        return {"status": "duplicate", "transaction_id": tx.transaction_id}
    entry = tx.model_dump()
    store[tx.transaction_id] = entry
    print(f"[logging] {datetime.now().isoformat()} STORED transaction_id={tx.transaction_id} user_id={tx.user_id} amount={tx.amount:+.2f}")
    return {"status": "stored", "transaction_id": tx.transaction_id}


@app.get("/messages")
async def get_all_transactions():
    result = list(store.values())
    print(f"[logging] GET /messages -> {len(result)} transactions")
    return result


@app.get("/user/{user_id}")
async def get_user_transactions(user_id: str):
    txs = [v for v in store.values() if v["user_id"] == user_id]
    print(f"[logging] GET /user/{user_id} -> {len(txs)} transactions")
    return txs


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
