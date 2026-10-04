"""Offline features must not require unrelated service credentials."""

import pytest

from config.season import resolve_focus_ids, season_bounds, validate_season_dates
from config.settings import Settings


def test_missing_credentials_only_fail_when_feature_is_requested(monkeypatch):
    for name in ("GEMINI_API_KEY", "YOUTUBE_API_KEY", "BALLDONTLIE_API_KEY", "NEO4J_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    settings = Settings(_env_file=None)
    assert settings.season == "2025-26"
    assert settings.gemini_api_key == ""
    with pytest.raises(ValueError, match="GEMINI_API_KEY"):
        settings.require_credentials("gemini_api_key")


@pytest.mark.parametrize("value", ["2025-25", "25-26", "2025-2026", "anything"])
def test_malformed_season_is_rejected(value):
    with pytest.raises(ValueError):
        season_bounds(value)


def test_boundary_dates_are_season_safe():
    validate_season_dates(["2025-07-01", "2026-06-30"], "2025-26")
    with pytest.raises(ValueError):
        validate_season_dates(["2026-07-01"], "2025-26")


@pytest.mark.parametrize("values", [[], [""], ["unknown"]])
def test_missing_date_evidence_is_rejected(values):
    with pytest.raises(ValueError):
        validate_season_dates(values, "2025-26")


def test_duplicate_or_missing_team_names_are_rejected():
    with pytest.raises(ValueError):
        resolve_focus_ids([{"id": 1, "name": "Liverpool"}] * 2, {"Liverpool"})
    with pytest.raises(ValueError):
        resolve_focus_ids([], {"Liverpool"})


def test_backend_health_imports_without_provider_credentials(tmp_path):
    import os
    import subprocess
    import sys
    from pathlib import Path

    project = Path(__file__).resolve().parents[2]
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"GEMINI_API_KEY", "YOUTUBE_API_KEY", "BALLDONTLIE_API_KEY", "NEO4J_PASSWORD"}
    }
    environment["PYTHONPATH"] = str(project)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from fastapi.testclient import TestClient; "
            "from backend.main import app; "
            "assert TestClient(app).get('/api/health').json() == {'status': 'ok'}",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
