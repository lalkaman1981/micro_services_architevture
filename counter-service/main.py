from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
import uvicorn
from datetime import datetime

app = FastAPI(title="counter-service")


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float  # positive = credit, negative = debit


# In-memory store: user_id -> balance
balances: Dict[str, float] = {}


@app.post("/transaction")
async def apply_transaction(tx: Transaction):
    prev = balances.get(tx.user_id, 0.0)
    balances[tx.user_id] = prev + tx.amount
    new_bal = balances[tx.user_id]
    print(f"[counter] {datetime.now().isoformat()} transaction_id={tx.transaction_id} user_id={tx.user_id} amount={tx.amount:+.2f} balance={new_bal:.2f}")
    return {"user_id": tx.user_id, "balance": new_bal}


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
