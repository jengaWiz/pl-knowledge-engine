"""
Tests for BallDontLie EPL API client (src/ingest/stats_api.py).

All external HTTP calls are mocked so these tests run offline.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingest.stats_api import BallDontLieClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Return a BallDontLieClient with paths redirected to a temp directory."""
    monkeypatch.setenv("BALLDONTLIE_API_KEY", "fake-key-for-ci-testing")
    monkeypatch.setenv("GEMINI_API_KEY", "fake-key-for-ci-testing")
    monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key-for-ci-testing")
    monkeypatch.setenv("NEO4J_PASSWORD", "fake-password-for-ci-testing")
    # We patch settings paths BEFORE importing settings in the client
    with patch("src.ingest.stats_api.settings") as mock_settings:
        mock_settings.balldontlie_api_key = "fake-key-for-ci-testing"
        mock_settings.raw_dir = tmp_path / "raw"
        mock_settings.checkpoint_dir = tmp_path / "checkpoints"
        c = BallDontLieClient.__new__(BallDontLieClient)
        c.session = MagicMock()
        c.raw_stats_dir = tmp_path / "raw" / "stats"
        c.lineups_dir = tmp_path / "raw" / "stats" / "lineups"
        c.raw_stats_dir.mkdir(parents=True, exist_ok=True)
        c.lineups_dir.mkdir(parents=True, exist_ok=True)
        yield c


def _make_response(data: list, next_cursor: int | None = None) -> MagicMock:
    """Build a mock requests.Response that returns paginated API data."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "data": data,
        "meta": {"next_cursor": next_cursor},
    }
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


# ---------------------------------------------------------------------------
# Pagination tests
# ---------------------------------------------------------------------------


class TestPagination:
    """Tests for cursor-based pagination in _get_all_pages."""

    def test_single_page(self, client):
        """Should return all data records when there is only one page."""
        page1 = _make_response([{"id": 1}, {"id": 2}], next_cursor=None)
        client.session.get.return_value = page1

        with patch.object(client, "_get", wraps=client._get):
            # Directly test _get_all_pages by mocking _get
            client.session.get.return_value = page1
            # Call _get directly to test page logic
            client.session.get.side_effect = [page1]

        # Re-mock _get to return our controlled response
        with patch.object(client, "_get", return_value={"data": [{"id": 1}, {"id": 2}], "meta": {"next_cursor": None}}):
            result = client._get_all_pages("/teams")

        assert len(result) == 2
        assert result[0]["id"] == 1

    def test_multi_page_pagination(self, client):
        """Should concatenate records from all pages until no next_cursor."""
        page1_data = {"data": [{"id": 1}, {"id": 2}], "meta": {"next_cursor": 100}}
        page2_data = {"data": [{"id": 3}], "meta": {"next_cursor": None}}

        with patch.object(client, "_get", side_effect=[page1_data, page2_data]):
            result = client._get_all_pages("/matches", {"season": 2025})

        assert len(result) == 3
        assert result[2]["id"] == 3

    def test_cursor_passed_to_next_request(self, client):
        """The cursor value from meta should be forwarded as a query param."""
        page1_data = {"data": [{"id": 1}], "meta": {"next_cursor": 42}}
        page2_data = {"data": [{"id": 2}], "meta": {"next_cursor": None}}
        mock_get = MagicMock(side_effect=[page1_data, page2_data])

        with patch.object(client, "_get", mock_get):
            client._get_all_pages("/fixtures")

        # Second call should pass cursor=42
        second_call_params = mock_get.call_args_list[1][0][1]
        assert second_call_params.get("cursor") == 42


# ---------------------------------------------------------------------------
# Team filtering tests
# ---------------------------------------------------------------------------


class TestTeamFiltering:
    """Tests for filtering matches to only focus teams."""

    def test_match_involves_aston_villa_home(self, client):
        """Should return True when Aston Villa is the home team."""
        match = {"home_team": {"name": "Aston Villa"}, "away_team": {"name": "Arsenal"}}
        assert client._match_involves_team(match, "Aston Villa") is True

    def test_match_involves_liverpool_away(self, client):
        """Should return True when Liverpool is the away team."""
        match = {"home_team": {"name": "Man City"}, "away_team": {"name": "Liverpool"}}
        assert client._match_involves_team(match, "Liverpool") is True

    def test_match_does_not_involve_team(self, client):
        """Should return False when neither side is the focus team."""
        match = {"home_team": {"name": "Arsenal"}, "away_team": {"name": "Chelsea"}}
        assert client._match_involves_team(match, "Aston Villa") is False

    def test_fetch_matches_filters_correctly(self, client, tmp_path):
        """fetch_matches should split records into team-specific lists."""
        all_matches = [
            {"id": 1, "home_team": {"name": "Aston Villa"}, "away_team": {"name": "Arsenal"}},
            {"id": 2, "home_team": {"name": "Man City"}, "away_team": {"name": "Liverpool"}},
            {"id": 3, "home_team": {"name": "Aston Villa"}, "away_team": {"name": "Liverpool"}},
            {"id": 4, "home_team": {"name": "Chelsea"}, "away_team": {"name": "Tottenham"}},
        ]
        with patch.object(client, "_get_all_pages", return_value=all_matches):
            villa_matches, liverpool_matches = client.fetch_matches()

        # Match 4 (Chelsea vs Tottenham) should be excluded
        villa_ids = {m["id"] for m in villa_matches}
        liverpool_ids = {m["id"] for m in liverpool_matches}
        assert villa_ids == {1, 3}
        assert liverpool_ids == {2, 3}


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Tests for rate-limit and HTTP error handling."""

    def test_rate_limit_response_retries(self, client):
        """A 429 response should trigger a retry after the Retry-After period."""
        rate_limit_resp = MagicMock()
        rate_limit_resp.status_code = 429
        rate_limit_resp.headers = {"Retry-After": "1"}
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {"data": [{"id": 99}], "meta": {}}
        success_resp.raise_for_status = MagicMock()

        client.session.get.side_effect = [rate_limit_resp, success_resp]

        with patch("src.ingest.stats_api.time.sleep"):
            result = client._get("/teams")

        assert result["data"][0]["id"] == 99

    def test_http_error_raises(self, client):
        """A server error (500) should propagate after max retries."""
        import requests

        error_resp = MagicMock()
        error_resp.status_code = 500
        error_resp.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        client.session.get.return_value = error_resp

        # The retry decorator will re-raise after max_attempts
        with patch("src.ingest.stats_api.time.sleep"):
            with pytest.raises(requests.HTTPError):
                client._get("/teams")
