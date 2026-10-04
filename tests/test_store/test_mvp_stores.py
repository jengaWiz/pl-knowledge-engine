"""Persistent indexing, stale evidence, and failed readiness regressions."""

import json

import pytest

from scripts import load_mvp
from src.store import mvp_graph, mvp_index


def prepare(root, monkeypatch):
    corpus = {
        "matches": [],
        "players": [
            {
                "id": "player1",
                "season": "2025-26",
                "name": "Example Player",
                "web_name": "Example",
                "position": "MID",
                "teams_represented": ["Aston Villa", "Liverpool"],
                "source": {"url": "https://example.org/source", "sha256": "abc", "row": 2},
            }
        ],
        "appearances": [{"player_id": "player1", "goals": 2, "assists": 1, "minutes": 90}],
    }
    monkeypatch.setattr(mvp_index, "load_verified_corpus", lambda *args: corpus)
    path = root / "reports/mvp/2025-26"
    path.mkdir(parents=True)
    (path / "quality.json").write_text(json.dumps({"artifact_sha256": {"players": "abc"}}))
    return path


def vector(texts):
    return [[1.0] + [0.0] * 383 for _ in texts]


def test_persistent_index_reload_and_transfer_filter(tmp_path, monkeypatch):
    path = prepare(tmp_path, monkeypatch)
    first = mvp_index.build_index(tmp_path, "2025-26", embed=vector)
    second = mvp_index.build_index(tmp_path, "2025-26", embed=vector)
    assert first == second and second["documents"] == 1
    (path / "stores.json").write_text(json.dumps({"valid": True, "index": second}))
    monkeypatch.setattr(mvp_index, "embedding_function", lambda: vector)
    hits = mvp_index.search(tmp_path, "2025-26", "Example", team="Liverpool")
    assert hits[0]["id"] == "player1"
    assert hits[0]["metadata"]["url"] == "https://example.org/source"
    assert mvp_index.search(tmp_path, "2025-26", "Example", team="Arsenal") == []
    (path / "quality.json").write_text(json.dumps({"artifact_sha256": {"players": "changed"}}))
    with pytest.raises(ValueError, match="stale"):
        mvp_index.search(tmp_path, "2025-26", "Example")


@pytest.mark.parametrize("embed", [lambda texts: [[1.0]], lambda texts: []])
def test_incompatible_vectors_fail_gate(tmp_path, monkeypatch, embed):
    prepare(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="dimensions or batch count"):
        mvp_index.build_index(tmp_path, "2025-26", embed=embed)


def test_model_contract_change_rejected(monkeypatch):
    monkeypatch.setattr(mvp_index.ONNXMiniLM_L6_V2, "_MODEL_SHA256", "changed")
    with pytest.raises(ValueError, match="contract changed"):
        mvp_index.embedding_function()


def test_failed_load_replaces_prior_ready_manifest(tmp_path, monkeypatch):
    path = tmp_path / "reports/mvp/2025-26"
    path.mkdir(parents=True)
    (path / "stores.json").write_text('{"valid": true}')
    monkeypatch.setattr(load_mvp.settings, "neo4j_password", "")
    with pytest.raises(ValueError):
        load_mvp.load_stores("2025-26", tmp_path)
    report = json.loads((path / "stores.json").read_text())
    assert not report["valid"] and "error" in report


def test_graph_requires_credentials_before_accessing_data(tmp_path):
    with pytest.raises(ValueError, match="NEO4J_PASSWORD"):
        mvp_graph.load_graph(tmp_path, "2025-26", "bolt://localhost:1", "neo4j", "")


def test_query_rejects_manifest_model_mismatch(tmp_path, monkeypatch):
    path = prepare(tmp_path, monkeypatch)
    index = mvp_index.build_index(tmp_path, "2025-26", embed=vector)
    index["dimensions"] = 3072
    (path / "stores.json").write_text(json.dumps({"valid": True, "index": index}))
    with pytest.raises(ValueError, match="query contract"):
        mvp_index.search(tmp_path, "2025-26", "Example")


@pytest.mark.parametrize("query,limit", [(" ", 5), ("Example", 0), ("Example", 51)])
def test_query_bounds_checked_before_data_access(tmp_path, query, limit):
    with pytest.raises(ValueError, match="nonempty query"):
        mvp_index.search(tmp_path, "2025-26", query, limit=limit)
