import importlib.util
import pathlib
import sys
import pytest
from fastapi.testclient import TestClient
import httpx


def load_app(path_rel: str):
    root = pathlib.Path(__file__).resolve().parents[2]
    p = root / path_rel
    spec = importlib.util.spec_from_file_location(p.stem, str(p))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return getattr(mod, "app")


logging_app = load_app("micro_basics/logging-service/main.py")
messages_app = load_app("micro_basics/messages-service/main.py")
facade_app = load_app("micro_basics/facade-service/main.py")


def test_logging_store_and_dedup():
    client = TestClient(logging_app)
    payload = {"id": "test-id-1", "msg": "hello"}
    r = client.post("/log", json=payload)
    assert r.status_code == 200
    assert r.json() == {"status": "stored", "id": "test-id-1"}

    # duplicate
    r2 = client.post("/log", json=payload)
    assert r2.status_code == 200
    assert r2.json() == {"status": "duplicate", "id": "test-id-1"}

    # check stored messages
    r3 = client.get("/messages")
    assert r3.status_code == 200
    assert "hello" in r3.text


def test_messages_service_message():
    client = TestClient(messages_app)
    r = client.get("/message")
    assert r.status_code == 200
    assert "not implemented yet" in r.text


def test_facade_post_success_and_retry(monkeypatch):
    async def mock_post_success(self, url, *args, **kwargs):
        req = httpx.Request("POST", str(url))
        return httpx.Response(200, json={"status": "stored", "id": "abc"}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "post", mock_post_success)
    client = TestClient(facade_app)
    r = client.post("/messages", json={"msg": "hi"})
    assert r.status_code == 200
    data = r.json()
    assert "id" in data
    assert isinstance(data["id"], str)
    assert data["status"]["status"] == "stored"

    # Simulate logging-service failure -> facade should return 502
    async def mock_post_fail(self, url, *args, **kwargs):
        req = httpx.Request("POST", str(url))
        return httpx.Response(500, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "post", mock_post_fail)
    r2 = client.post("/messages", json={"msg": "failcase"})
    assert r2.status_code == 502


def test_facade_get_combined(monkeypatch):
    async def mock_get_logging(self, url, *args, **kwargs):
        if url.endswith("/messages"):
            return httpx.Response(200, content="log1\nlog2")
        return httpx.Response(404)

    async def mock_get_messages(self, url, *args, **kwargs):
        if url.endswith("/message"):
            return httpx.Response(200, content="not implemented yet")
        return httpx.Response(404)

    # monkeypatch AsyncClient.get to look at url and return appropriate response
    async def mock_get(self, url, *args, **kwargs):
        req = httpx.Request("GET", str(url))
        if "/messages" in str(url):
            return httpx.Response(200, content="log1\nlog2", request=req)
        if "/message" in str(url):
            return httpx.Response(200, content="not implemented yet", request=req)
        return httpx.Response(404, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)
    client = TestClient(facade_app)
    r = client.get("/messages")
    assert r.status_code == 200
    assert "log1" in r.text
    assert "not implemented yet" in r.text
