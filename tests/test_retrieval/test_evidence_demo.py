"""Expanded Docker launcher uses persistent app services and reports retryable status."""

from unittest.mock import MagicMock

from scripts import demo


def test_evidence_launcher_uses_named_volume_container_and_skips_ready_mvp(tmp_path, monkeypatch):
    monkeypatch.setattr(demo, "ROOT", tmp_path)
    calls = []
    monkeypatch.setattr(demo.subprocess, "run", lambda args, **kwargs: calls.append(args))

    def status(path="/api/readiness", **kwargs):
        return {
            "status": "ready",
            "index": {"documents": 2682 if "evidence" in path else 457},
            "seasons": ["2022-23", "2023-24", "2024-25", "2025-26"],
            "counts": {},
        }

    monkeypatch.setattr(demo, "status", status)
    demo.run_demo("evidence")
    assert any("--build" in call for call in calls)
    preparation = [call for call in calls if "scripts/prepare_evidence.py" in call]
    assert len(preparation) == 1 and ["run", "--rm", "--no-deps", "app"] == preparation[0][-6:-2]
    assert not any("scripts/load_mvp.py" in call for call in calls)


def test_evidence_status_does_not_generate_credentials(tmp_path, monkeypatch):
    monkeypatch.setattr(demo, "ROOT", tmp_path)
    monkeypatch.setattr(demo, "status", lambda *args, **kwargs: {"status": "not_ready"})
    demo.run_demo("evidence-status")
    assert not (tmp_path / "data").exists()


def test_busy_status_is_not_reported_ready(monkeypatch):
    from io import BytesIO
    from urllib.error import HTTPError

    error = HTTPError(
        "http://local/api/evidence/status", 429, "Busy", {}, BytesIO(b'{"detail":"Retry shortly"}')
    )
    monkeypatch.setattr(demo, "urlopen", MagicMock(side_effect=error))
    assert demo.status("/api/evidence/status")["status"] == "not_ready"


def test_cold_launcher_finishes_mvp_gates_before_expanded_preparation(tmp_path, monkeypatch):
    monkeypatch.setattr(demo, "ROOT", tmp_path)
    calls = []
    monkeypatch.setattr(demo.subprocess, "run", lambda args, **kwargs: calls.append(args))
    reports = iter(
        [
            {"status": "not_ready"},
            {"status": "ready", "counts": {}, "index": {"documents": 457}},
            {"status": "ready", "seasons": ["2025-26"], "index": {"documents": 2682}},
        ]
    )
    monkeypatch.setattr(demo, "status", lambda *args, **kwargs: next(reports))
    demo.run_demo("evidence")
    stages = [call[-1] for call in calls if "python" in call]
    assert stages == [
        "--commentary",
        "scripts/load_mvp.py",
        "scripts/accept_mvp.py",
        "scripts/prepare_evidence.py",
    ]
    assert "scripts/run_mvp.py" in calls[1]
