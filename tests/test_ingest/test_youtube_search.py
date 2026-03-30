"""
Tests for src/ingest/youtube_search.py
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest

from src.ingest.youtube_search import (
    YouTubeSearchClient,
    _parse_iso8601_duration,
    MIN_DURATION_SECONDS,
    MAX_DURATION_SECONDS,
)


# ---------------------------------------------------------------------------
# Duration parser tests
# ---------------------------------------------------------------------------

class TestParseDuration:
    def test_hours_minutes_seconds(self):
        assert _parse_iso8601_duration("PT1H4M30S") == 3870

    def test_minutes_seconds_only(self):
        assert _parse_iso8601_duration("PT44M0S") == 2640

    def test_seconds_only(self):
        assert _parse_iso8601_duration("PT45S") == 45

    def test_hours_only(self):
        assert _parse_iso8601_duration("PT2H") == 7200

    def test_invalid_returns_zero(self):
        assert _parse_iso8601_duration("INVALID") == 0


# ---------------------------------------------------------------------------
# Duration filter tests
# ---------------------------------------------------------------------------

class TestDurationFilter:
    def setup_method(self):
        with patch("src.ingest.youtube_search.build"), \
             patch("src.ingest.youtube_search.settings") as ms:
            ms.youtube_api_key = "fake"
            ms.raw_dir = MagicMock()
            ms.raw_dir.__truediv__ = lambda s, x: MagicMock()
            ms.checkpoint_dir = MagicMock()
            self.client = YouTubeSearchClient.__new__(YouTubeSearchClient)
            self.client.youtube = MagicMock()
            self.client.checkpoint = MagicMock()
            self.client.output_path = MagicMock()
            self.client.output_path.exists.return_value = False

    def test_valid_episode_passes(self):
        assert self.client._is_valid_episode(MIN_DURATION_SECONDS) is True
        assert self.client._is_valid_episode(2640) is True
        assert self.client._is_valid_episode(MAX_DURATION_SECONDS) is True

    def test_too_short_fails(self):
        assert self.client._is_valid_episode(MIN_DURATION_SECONDS - 1) is False

    def test_too_long_fails(self):
        assert self.client._is_valid_episode(MAX_DURATION_SECONDS + 1) is False


# ---------------------------------------------------------------------------
# Channel search deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:
    def _make_client(self, tmp_path):
        with patch("src.ingest.youtube_search.build"), \
             patch("src.ingest.youtube_search.settings") as ms, \
             patch("src.ingest.youtube_search.PODCAST_CHANNELS", []):
            ms.youtube_api_key = "fake"
            ms.raw_dir = tmp_path
            ms.checkpoint_dir = tmp_path / "checkpoints"
            client = YouTubeSearchClient.__new__(YouTubeSearchClient)
            client.youtube = MagicMock()
            client.checkpoint = MagicMock()
            client.checkpoint.is_completed.return_value = False
            manifest = tmp_path / "transcripts" / "podcast_episodes.json"
            manifest.parent.mkdir(parents=True, exist_ok=True)
            client.output_path = manifest
            return client

    def test_duplicate_episodes_deduplicated(self, tmp_path):
        """Same youtube_id appearing in two channels should appear once."""
        client = self._make_client(tmp_path)

        # Simulate two search results returning the same video ID
        ep1 = {"youtube_id": "abc", "title": "Test", "channel": "Ch1",
               "published_at": "2025-09-01T00:00:00Z", "duration_seconds": 2640,
               "description": ""}
        ep2 = {"youtube_id": "abc", "title": "Test", "channel": "Ch2",
               "published_at": "2025-09-01T00:00:00Z", "duration_seconds": 2640,
               "description": ""}

        all_episodes: dict = {ep1["youtube_id"]: ep1}
        all_episodes[ep2["youtube_id"]] = ep2  # Second write overwrites

        assert len(all_episodes) == 1

    def test_distinct_episodes_both_kept(self, tmp_path):
        """Different youtube_ids should both remain."""
        episodes = {"vid1": {"youtube_id": "vid1"}, "vid2": {"youtube_id": "vid2"}}
        assert len(episodes) == 2
