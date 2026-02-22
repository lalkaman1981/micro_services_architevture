import uuid
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uvicorn
from datetime import datetime

app = FastAPI(title="facade-service")

LOGGING_URL = "http://localhost:8001/log"
MESSAGES_URL = "http://localhost:8002/message"


class MsgIn(BaseModel):
    msg: str


async def post_with_retry(payload, retries=3, backoff_base=0.5):
    async with httpx.AsyncClient(timeout=5.0) as client:
        attempt = 0
        while True:
            try:
                attempt += 1
                print(f"[facade] {datetime.now().isoformat()} POST attempt {attempt}/{retries} for id={payload.get('id')}")
                r = await client.post(LOGGING_URL, json=payload)
                r.raise_for_status()
                if attempt > 1:
                    print(f"[facade] {datetime.now().isoformat()} RETRY SUCCESS after {attempt} attempts for id={payload.get('id')}")
                else:
                    print(f"[facade] {datetime.now().isoformat()} SUCCESS on first attempt for id={payload.get('id')}")
                return r.json()
            except Exception as e:
                print(f"[facade] {datetime.now().isoformat()} POST attempt {attempt} failed: {e}")
                if attempt >= retries:
                    print(f"[facade] {datetime.now().isoformat()} FAILED after {attempt} attempts for id={payload.get('id')}")
                    raise
                backoff_delay = backoff_base * (2 ** (attempt - 1))
                print(f"[facade] {datetime.now().isoformat()} waiting {backoff_delay}s before retry...")
                await asyncio.sleep(backoff_delay)


@app.post("/messages")
async def add_message(item: MsgIn):
    msg_id = str(uuid.uuid4())
    payload = {"id": msg_id, "msg": item.msg}
    print(f"[facade] {datetime.now().isoformat()} POST /messages received - id={msg_id}, msg={item.msg}")
    try:
        res = await post_with_retry(payload, retries=4)
        print(f"[facade] {datetime.now().isoformat()} POST /messages response - id={msg_id}, status={res.get('status')}")
    except Exception as e:
        print(f"[facade] {datetime.now().isoformat()} POST /messages FAILED - id={msg_id}, error={e}")
        raise HTTPException(status_code=502, detail=f"Failed sending to logging-service: {e}")
    return {"id": msg_id, "status": res}


@app.get("/messages")
async def get_messages():
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            r1 = await client.get("http://localhost:8001/messages")
            r1.raise_for_status()
            logging_msgs = r1.text
            print(f"[facade] retrieved messages from logging-service")
        except Exception as e:
            print(f"[facade] failed to get from logging-service: {e}")
            raise HTTPException(status_code=502, detail=f"Failed to reach logging-service: {e}")
        
        try:
            r2 = await client.get(MESSAGES_URL)
            r2.raise_for_status()
            messages_msgs = r2.text
            print(f"[facade] retrieved message from messages-service")
        except Exception as e:
            print(f"[facade] failed to get from messages-service: {e}")
            raise HTTPException(status_code=502, detail=f"Failed to reach messages-service: {e}")
        
        combined = f"{logging_msgs}\n{messages_msgs}".strip()
        return combined


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
