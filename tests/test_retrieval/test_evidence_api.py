"""Optional retrieval API preserves explicit scope and honest readiness/errors."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from neo4j.exceptions import ServiceUnavailable

from backend import evidence
from backend.main import app


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(evidence.settings, "neo4j_password", "test")
    monkeypatch.setattr(evidence, "model_cached", lambda: True)
    return TestClient(app)


@pytest.mark.parametrize(
    "body",
    [
        {"query": "fixture"},
        {"query": " ", "evidence_season": "2025-26"},
        {"query": "x" * 4001, "evidence_season": "2025-26"},
        {"query": "fixture", "evidence_season": "2025-27"},
        {"query": "fixture", "evidence_season": "2025-26", "limit": 21},
        {"query": "fixture", "evidence_season": "2025-26", "limit": True},
        {"query": "fixture", "evidence_season": "2025-26", "limit": 1.5},
        {"query": "fixture", "evidence_season": "2025-26", "history": []},
    ],
)
def test_request_bounds_before_store_access(client, monkeypatch, body):
    retrieve = MagicMock()
    monkeypatch.setattr(evidence, "retrieve", retrieve)
    assert client.post("/api/evidence/retrieve", json=body).status_code == 422
    retrieve.assert_not_called()


@pytest.mark.parametrize("status", ["resolved", "clarification", "unsupported", "unavailable"])
def test_response_preserves_route_and_explicit_season(client, monkeypatch, status):
    result = {"route": {"status": status, "season": "2023-24", "reason": "Verified"}, "hits": []}
    retrieve = MagicMock(return_value=result)
    monkeypatch.setattr(evidence, "retrieve", retrieve)
    response = client.post(
        "/api/evidence/retrieve",
        json={"query": "fixture", "evidence_season": "2023-24", "limit": 3},
    )
    assert response.status_code == 200 and response.json() == result
    assert retrieve.call_args.kwargs == {"evidence_season": "2023-24", "limit": 3}


@pytest.mark.parametrize(
    "error", [ValueError("secret-password"), ServiceUnavailable("secret-password")]
)
def test_store_errors_hide_secrets(client, monkeypatch, error):
    monkeypatch.setattr(evidence, "retrieve", MagicMock(side_effect=error))
    response = client.post(
        "/api/evidence/retrieve", json={"query": "fixture", "evidence_season": "2025-26"}
    )
    assert response.status_code == 503 and "secret-password" not in response.text


def test_cold_model_never_downloads_or_retrieves(client, monkeypatch):
    monkeypatch.setattr(evidence, "model_cached", lambda: False)
    retrieve = MagicMock()
    monkeypatch.setattr(evidence, "retrieve", retrieve)
    assert (
        client.post(
            "/api/evidence/retrieve", json={"query": "fixture", "evidence_season": "2025-26"}
        ).status_code
        == 503
    )
    retrieve.assert_not_called()


@pytest.mark.parametrize("path", ["/api/evidence/status", "/api/evidence/retrieve"])
def test_busy_requests_are_retryable(client, path):
    assert evidence.WORK.acquire(blocking=False)
    try:
        response = (
            client.get(path)
            if path.endswith("status")
            else client.post(path, json={"query": "fixture", "evidence_season": "2025-26"})
        )
        assert response.status_code == 429 and response.headers["Retry-After"] == "2"
    finally:
        evidence.WORK.release()


@pytest.fixture
def ready(client, monkeypatch):
    plan = {
        "dataset_id": "graph",
        "counts": {"Entity": 1},
        "nodes": [
            {"props": {"entity_type": "Match", "season": "2023-24", "canonical_id": "match"}}
        ],
    }
    monkeypatch.setattr(evidence, "prepare_graph", MagicMock(return_value=plan))
    monkeypatch.setattr(evidence, "read", MagicMock(return_value=[{"document_id": "doc"}]))
    monkeypatch.setattr(
        evidence.evidence_index,
        "status",
        MagicMock(
            return_value={"records_by_season": {"2025-26": 1542, "2023-24": 380}, "documents": 1922}
        ),
    )
    return client


def test_verified_readiness_advertises_only_supported_seasons(ready):
    response = ready.get("/api/evidence/status")
    assert response.status_code == 200
    assert response.json()["seasons"] == ["2023-24", "2025-26"]
    assert all(response.json()["checks"].values())


def test_model_missing_withdraws_ready_seasons(ready, monkeypatch):
    monkeypatch.setattr(evidence, "model_cached", lambda: False)
    response = ready.get("/api/evidence/status")
    assert response.status_code == 503 and response.json()["seasons"] == []
    assert response.json()["checks"]["model"] is False


def test_changed_sources_withdraw_readiness(ready, monkeypatch):
    initial = evidence.prepare_graph()
    monkeypatch.setattr(
        evidence,
        "prepare_graph",
        MagicMock(side_effect=[initial, {**initial, "dataset_id": "changed"}]),
    )
    assert ready.get("/api/evidence/status").status_code == 503


def test_missing_optional_corpus_does_not_break_health_or_default_chat(client, monkeypatch):
    monkeypatch.setattr(evidence, "prepare_graph", MagicMock(side_effect=ValueError("secret")))
    response = client.get("/api/evidence/status")
    assert response.status_code == 503 and "secret" not in response.text
    assert client.get("/api/health").status_code == 200


@pytest.mark.parametrize(
    "missing",
    [
        None,
        "config.json",
        "model.onnx",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "tokenizer.json",
        "vocab.txt",
    ],
)
def test_partial_model_cache_never_advertises_ready(tmp_path, monkeypatch, missing):
    monkeypatch.setattr(evidence.ONNXMiniLM_L6_V2, "DOWNLOAD_PATH", tmp_path)
    folder = tmp_path / evidence.ONNXMiniLM_L6_V2.EXTRACTED_FOLDER_NAME
    folder.mkdir()
    for name in (
        "config.json",
        "model.onnx",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "tokenizer.json",
        "vocab.txt",
    ):
        if name != missing:
            (folder / name).write_text("cached")
    assert evidence.model_cached() == (missing is None)
