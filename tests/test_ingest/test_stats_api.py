"""Exercise the current FPL adapter offline, including wrong-season rejection."""

import json
from unittest.mock import Mock, patch

import pytest
import requests

from src.ingest.stats_api import FPLMatchClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ingest.stats_api.settings.raw_dir", tmp_path)
    return FPLMatchClient()


@pytest.fixture
def teams():
    # Deliberately different IDs from the old hard-coded season configuration.
    return [
        {"id": 77, "name": "Aston Villa", "short_name": "AVL"},
        {"id": 88, "name": "Liverpool", "short_name": "LIV"},
        {"id": 99, "name": "Arsenal", "short_name": "ARS"},
    ]


def fixture(home, away, ident):
    return {
        "id": ident,
        "team_h": home,
        "team_a": away,
        "kickoff_time": "2025-09-01T15:00:00Z",
        "event": 3,
        "team_h_score": 2,
        "team_a_score": 1,
        "finished": True,
    }


def test_fetch_teams_requires_dated_season_evidence(client, teams):
    with patch.object(
        client,
        "_get",
        return_value={"teams": teams, "events": [{"deadline_time": "2025-08-15T17:00:00Z"}]},
    ):
        assert client.fetch_teams() == teams
    assert json.loads((client.raw_stats_dir / "teams.json").read_text()) == teams


@pytest.mark.parametrize("events", [[], [{"deadline_time": "2026-08-15T17:00:00Z"}]])
def test_wrong_or_unknown_season_is_not_saved(client, teams, events):
    with patch.object(client, "_get", return_value={"teams": teams, "events": events}):
        with pytest.raises(ValueError):
            client.fetch_teams()
    assert not (client.raw_stats_dir / "teams.json").exists()


def test_matches_use_source_ids_and_retain_both_team_membership(client, teams):
    rows = [fixture(77, 99, 1), fixture(99, 88, 2), fixture(77, 88, 3)]
    with patch.object(client, "_get", return_value=rows):
        villa, liverpool = client.fetch_matches(teams)
    assert {row["id"] for row in villa} == {1, 3}
    assert {row["id"] for row in liverpool} == {2, 3}
    assert villa[0]["home_team_name"] == "Aston Villa"
    assert villa[0]["match_date"] == "2025-09-01"


def test_wrong_season_fixtures_are_rejected_before_write(client, teams):
    row = fixture(77, 88, 1)
    row["kickoff_time"] = "2026-09-01T15:00:00Z"
    with patch.object(client, "_get", return_value=[row]):
        with pytest.raises(ValueError, match="configured season"):
            client.fetch_matches(teams)
    assert not (client.raw_stats_dir / "matches_villa.json").exists()


def test_http_retry_is_bounded_and_preserves_timeout(client):
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("temporary error")
    client.session = Mock()
    client.session.get.return_value = response
    with patch("src.utils.retry.time.sleep"):
        with pytest.raises(requests.HTTPError):
            client._get("/fixtures/")
    assert client.session.get.call_count == 5
    assert client.session.get.call_args.kwargs["timeout"] == 30
