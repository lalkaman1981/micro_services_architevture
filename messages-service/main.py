from fastapi import FastAPI
import uvicorn
from datetime import datetime

app = FastAPI(title="messages-service")


@app.get("/message")
async def message():
    print(f"[messages-service] {datetime.now().isoformat()} GET /message requested")
    return "not implemented yet"


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
