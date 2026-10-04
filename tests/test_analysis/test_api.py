"""The default API answers locally and validates requests without provider keys."""

import pytest
from fastapi.testclient import TestClient

from backend.main import app
from src.analysis import deductions


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(
        deductions,
        "answer",
        lambda *args: {
            "status": "ok",
            "season": "2025-26",
            "reply": "Local verified answer",
            "sources": [],
        },
    )
    return TestClient(app)


def test_default_chat_is_local(client):
    response = client.post("/api/chat", json={"message": "Liverpool points"})
    assert response.status_code == 200 and response.json()["reply"] == "Local verified answer"


@pytest.mark.parametrize(
    "body",
    [
        {"message": " "},
        {"message": "x" * 4097},
        {"message": "test", "history": [{"role": "system", "content": "test"}]},
        {"message": "test", "history": [{"role": "user", "content": "test"}] * 9},
    ],
)
def test_invalid_chat_requests_rejected(client, body):
    assert client.post("/api/chat", json=body).status_code == 422


def test_missing_dataset_returns_actionable_unavailable(client, monkeypatch):
    def fail(*args):
        raise FileNotFoundError("missing")

    monkeypatch.setattr(deductions, "answer", fail)
    response = client.post("/api/chat", json={"message": "Liverpool points"})
    assert response.status_code == 503 and "MVP pipeline" in response.json()["detail"]


def test_typed_analysis_and_query_bounds(client, monkeypatch):
    monkeypatch.setattr(deductions, "analyze", lambda *args, **kwargs: {"status": "ok", **kwargs})
    response = client.post("/api/analysis", json={"operation": "form", "teams": ["Liverpool"]})
    assert response.status_code == 200 and response.json()["operation"] == "form"
    assert client.post("/api/analysis", json={"operation": "prediction"}).status_code == 422
    assert (
        client.get("/api/evidence/search", params={"query": "test", "limit": 0}).status_code == 422
    )
