from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
import uvicorn
from datetime import datetime

app = FastAPI(title="logging-service")

class LogItem(BaseModel):
    id: str
    msg: str

store: Dict[str, str] = {}


@app.post("/log")
async def receive_log(item: LogItem):
    # deduplication
    if item.id in store:
        print(f"[logging-service] {datetime.now().isoformat()} DUPLICATE received, id={item.id}, msg={item.msg}")
        return {"status": "duplicate", "id": item.id}
    store[item.id] = item.msg
    print(f"[logging-service] {datetime.now().isoformat()} STORED id={item.id}, msg={item.msg}")
    return {"status": "stored", "id": item.id}


@app.get("/messages")
async def get_messages():
    msgs = list(store.values())
    result = "\n".join(msgs) if msgs else ""
    print(f"[logging-service] {datetime.now().isoformat()} GET /messages requested, returning {len(msgs)} messages")
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
