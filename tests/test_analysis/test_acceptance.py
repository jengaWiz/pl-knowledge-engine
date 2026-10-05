"""Acceptance reports must fail on changed references and untraceable results."""

import json

import pytest
from fastapi import HTTPException
from neo4j.exceptions import ServiceUnavailable

from backend import graph
from config.sources import load_sources
from scripts import accept_mvp, accept_stores


@pytest.fixture
def reference(tmp_path, monkeypatch):
    source = next(row for row in load_sources("2025-26") if row.id == "matches")
    artifact = tmp_path / "reference.json"
    artifact.write_text(
        json.dumps(
            {
                "season": "2025-26",
                "method": "Independent SQL",
                "source_sha256": {"matches": source.sha256},
                "cases": [
                    {
                        "question": "Points?",
                        "request": {"operation": "team_stats"},
                        "path": ["rows", 0, "points"],
                        "expected": 3,
                        "source_rows": [2],
                    }
                ],
            }
        )
    )
    report = tmp_path / "reports/mvp/2025-26"
    report.mkdir(parents=True)
    (report / "quality.json").write_text(
        json.dumps({"counts": {"matches": 1}, "artifact_sha256": {}, "warnings": []})
    )
    monkeypatch.setattr(
        accept_mvp, "load_verified_corpus", lambda *args: {"matches": [{"id": "m1"}]}
    )
    monkeypatch.setattr(accept_mvp, "answer", lambda *args: {"status": "insufficient_evidence"})
    result = {
        "rows": [{"points": 3}],
        "sources": [
            {
                "url": source.url,
                "source_sha256": source.sha256,
                "record_ids": ["m1"],
                "source_rows": [2],
            }
        ],
    }
    monkeypatch.setattr(accept_mvp, "analyze", lambda *args, **kwargs: result)
    return tmp_path, artifact, result


def test_valid_reference_report_is_durable(reference):
    root, refs, _ = reference
    report = accept_mvp.verify_references(root, "2025-26", refs)
    assert report["valid"]
    saved = json.loads((root / "reports/mvp/2025-26/acceptance.json").read_text())
    assert saved == report


def test_wrong_number_fails_reference_gate(reference):
    root, refs, result = reference
    result["rows"][0]["points"] = 4
    report = accept_mvp.verify_references(root, "2025-26", refs)
    assert not report["valid"] and report["checks"][0]["traceable"]


def test_unknown_record_fails_traceability(reference):
    root, refs, result = reference
    result["sources"][0]["record_ids"] = ["invented"]
    report = accept_mvp.verify_references(root, "2025-26", refs)
    assert not report["valid"] and not report["checks"][0]["traceable"]


def test_changed_contract_rejects_golden_reference(reference):
    root, refs, _ = reference
    data = json.loads(refs.read_text())
    data["source_sha256"]["matches"] = "changed"
    refs.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="source contracts"):
        accept_mvp.verify_references(root, "2025-26", refs)


def test_committed_reference_set_matches_pinned_sources():
    data = json.loads(accept_mvp.DEFAULT_REFERENCES.read_text())
    assert len(data["cases"]) >= 20
    assert {case["request"]["operation"] for case in data["cases"]} == {
        "team_stats",
        "form",
        "home_away",
        "player_rankings",
    }
    contracts = {source.id: source.sha256 for source in load_sources(data["season"])}
    assert all(contracts[key] == digest for key, digest in data["source_sha256"].items())


def test_graph_projection_excludes_internal_provenance_and_secrets():
    result = graph.node_view(
        {
            "id": "n1",
            "type": "Player",
            "props": {
                "web_name": "Example",
                "goals_scored": 2,
                "position": "MID",
                "source_json": "private internal record",
                "dataset_id": "version",
            },
        }
    )
    assert result == {
        "id": "n1",
        "type": "Player",
        "name": "Example",
        "goals": 2,
        "position": "MID",
    }


def test_database_outage_returns_actionable_503(monkeypatch):
    def fail():
        raise ServiceUnavailable("Connection refused")

    monkeypatch.setattr(graph, "connect", fail)
    with pytest.raises(HTTPException) as error:
        graph.read_graph("overview")
    assert error.value.status_code == 503 and "reload stores" in error.value.detail


def test_real_store_acceptance_is_loopback_only(tmp_path):
    with pytest.raises(ValueError, match="loopback"):
        accept_stores.verify_stores(tmp_path, "2025-26", "https://example.org")


def test_wrong_source_url_fails_traceability(reference):
    root, refs, result = reference
    result["sources"][0]["url"] = "https://example.org/invented"
    report = accept_mvp.verify_references(root, "2025-26", refs)
    assert not report["valid"] and not report["checks"][0]["traceable"]
