"""Preparation withdraws readiness on failure and revalidates caches on retry."""

import json
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from filelock import FileLock, Timeout

from backend import evidence
from backend.main import app
from scripts import prepare_evidence


def test_failed_stage_stops_and_records_failure_without_secret(tmp_path):
    calls = []

    def execute(script, flags):
        calls.append(script)
        if script == "index_evidence.py":
            raise RuntimeError("secret-token")

    with pytest.raises(RuntimeError):
        prepare_evidence.prepare(tmp_path, "2025-26", execute=execute)
    path = tmp_path / "reports/evidence/2025-26/pipeline.json"
    report = json.loads(path.read_text())
    assert report["status"] == "failed" and "secret-token" not in path.read_text()
    assert "load_evidence_graph.py" not in calls
    retried = []
    completed = prepare_evidence.prepare(
        tmp_path, "2025-26", execute=lambda script, flags: retried.append(script)
    )
    assert completed["status"] == "complete" and retried == [s for s, _ in prepare_evidence.STAGES]
    assert calls[0] == retried[0]  # Retry verifies inputs; it does not trust old completed stages.


def test_competing_preparation_preserves_current_report(tmp_path):
    folder = tmp_path / "reports/evidence/2025-26"
    folder.mkdir(parents=True)
    path = folder / "pipeline.json"
    path.write_text('{"status":"running"}')
    with FileLock(str(folder / "pipeline.lock")):
        with pytest.raises(Timeout):
            prepare_evidence.prepare(tmp_path, "2025-26", execute=MagicMock())
    assert path.read_text() == '{"status":"running"}'


@pytest.mark.parametrize("status", ["running", "failed", "unknown"])
def test_incomplete_preparation_blocks_api_before_store_access(tmp_path, monkeypatch, status):
    folder = tmp_path / "reports/evidence/2025-26"
    folder.mkdir(parents=True)
    (folder / "pipeline.json").write_text(
        json.dumps({"schema": "evidence-preparation-v1", "status": status, "season": "2025-26"})
    )
    monkeypatch.setattr(evidence.settings, "data_dir", tmp_path)
    retrieve = MagicMock()
    monkeypatch.setattr(evidence, "retrieve", retrieve)
    client = TestClient(app)
    assert client.get("/api/evidence/status").status_code == 503
    assert (
        client.post(
            "/api/evidence/retrieve", json={"query": "fixture", "evidence_season": "2025-26"}
        ).status_code
        == 503
    )
    retrieve.assert_not_called()
    assert client.get("/api/health").status_code == 200
