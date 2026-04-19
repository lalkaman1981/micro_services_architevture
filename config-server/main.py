from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, List
import uvicorn
from datetime import datetime

app = FastAPI(title="config-server")

registry: Dict[str, List[str]] = {}


class ServiceRegistration(BaseModel):
    service_name: str
    url: str


@app.post("/register")
async def register(reg: ServiceRegistration):
    if reg.service_name not in registry:
        registry[reg.service_name] = []
    if reg.url not in registry[reg.service_name]:
        registry[reg.service_name].append(reg.url)
        print(f"[config] {datetime.now().isoformat()} Registered {reg.service_name} at {reg.url}")
    return {"status": "registered", "service_name": reg.service_name, "url": reg.url}


@app.get("/services/{service_name}")
async def get_services(service_name: str):
    urls = registry.get(service_name, [])
    print(f"[config] {datetime.now().isoformat()} Lookup {service_name} -> {urls}")
    return {"service_name": service_name, "urls": urls}


@app.get("/registry")
async def get_registry():
    return registry


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
