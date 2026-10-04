"""Interruptions and mandatory failures cannot become successful checkpoints."""

import json
from unittest.mock import Mock, patch

import pytest
from filelock import FileLock, Timeout

from scripts.run_mvp import run_mvp
from src.utils.checkpoint import Checkpoint


def test_failed_stage_is_durable_and_prevents_downstream_execution(tmp_path):
    failed, later = Mock(side_effect=RuntimeError("download failed")), Mock()
    with pytest.raises(RuntimeError, match="download failed"):
        run_mvp("2025-26", tmp_path, stages={"matches": failed, "players": later})
    later.assert_not_called()
    report = json.loads((tmp_path / "reports/mvp/2025-26/pipeline.json").read_text())
    assert report["status"] == "failed" and report["stages"]["matches"]["status"] == "failed"


def test_failed_gate_cannot_be_marked_complete(tmp_path):
    with pytest.raises(ValueError, match="failed gate"):
        run_mvp("2025-26", tmp_path, stages={"quality": lambda: {"valid": False}})


def test_rerun_revalidates_stages_and_replaces_failed_status(tmp_path):
    with pytest.raises(ValueError):
        run_mvp("2025-26", tmp_path, stages={"quality": lambda: {"valid": False}})
    calls = Mock(return_value={"valid": True})
    report = run_mvp("2025-26", tmp_path, stages={"quality": calls})
    assert report["status"] == "complete"
    run_mvp("2025-26", tmp_path, stages={"quality": calls})
    assert calls.call_count == 2


def test_concurrent_run_cannot_replace_active_pipeline_report(tmp_path):
    target = tmp_path / "reports/mvp/2025-26"
    target.mkdir(parents=True)
    (target / "pipeline.json").write_text('{"status":"running"}')
    with FileLock(str(target / "pipeline.lock")):
        with pytest.raises(Timeout):
            run_mvp("2025-26", tmp_path, stages={})
    assert json.loads((target / "pipeline.json").read_text())["status"] == "running"


def test_checkpoint_write_failure_preserves_previous_state(tmp_path, monkeypatch):
    monkeypatch.setattr("src.utils.checkpoint.settings.checkpoint_dir", tmp_path)
    checkpoint = Checkpoint("test")
    checkpoint.mark_completed("first")
    with patch("src.utils.checkpoint.atomic_write", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            checkpoint.mark_completed("second")
    assert checkpoint.is_completed("first") and not checkpoint.is_completed("second")
    assert Checkpoint("test").completed == {"first"}


def test_malformed_checkpoint_fails_instead_of_skipping_arbitrary_items(tmp_path, monkeypatch):
    monkeypatch.setattr("src.utils.checkpoint.settings.checkpoint_dir", tmp_path)
    (tmp_path / "test.json").write_text('{"completed":"not a list"}')
    with pytest.raises(ValueError, match="checkpoint format"):
        Checkpoint("test")
