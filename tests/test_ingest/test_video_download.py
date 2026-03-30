"""
Tests for src/ingest/video_download.py
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch
import json
import pytest

from src.ingest.video_download import VideoHighlightDownloader, _parse_iso8601_duration


class TestParseDuration:
    def test_minutes_seconds(self):
        assert _parse_iso8601_duration("PT3M22S") == 202

    def test_hours_minutes(self):
        assert _parse_iso8601_duration("PT1H30M") == 5400

    def test_seconds_only(self):
        assert _parse_iso8601_duration("PT90S") == 90

    def test_invalid_returns_zero(self):
        assert _parse_iso8601_duration("GARBAGE") == 0


class TestVideoDownloader:
    def _make_downloader(self, tmp_path):
        with patch("src.ingest.video_download._check_ffmpeg"), \
             patch("src.ingest.video_download.build"), \
             patch("src.ingest.video_download.settings") as ms, \
             patch("src.ingest.video_download.Checkpoint"):
            ms.youtube_api_key = "fake"
            ms.raw_dir = tmp_path
            ms.cleaned_dir = tmp_path / "cleaned"
            ms.checkpoint_dir = tmp_path / "checkpoints"
            (tmp_path / "video").mkdir(parents=True, exist_ok=True)
            dl = VideoHighlightDownloader.__new__(VideoHighlightDownloader)
            dl.youtube = MagicMock()
            dl.video_dir = tmp_path / "video"
            dl.checkpoint = MagicMock()
            dl.checkpoint.is_completed.return_value = False
            dl.metadata = []
            return dl

    def test_duration_filter_rejects_too_short(self, tmp_path):
        """Videos shorter than 60s should not be downloaded."""
        dl = self._make_downloader(tmp_path)

        # Mock search returns one video
        dl.youtube.search.return_value.list.return_value.execute.return_value = {
            "items": [{"id": {"videoId": "vid1"}, "snippet": {"title": "Highlights"}}]
        }
        # Mock duration as 30s (too short)
        dl._get_video_duration = MagicMock(return_value=30)
        dl._download_video = MagicMock()

        match = {"id": "m1", "home_team": "Liverpool", "away_team": "Aston Villa"}
        result = dl.download_match_highlights(match)

        assert result is False
        dl._download_video.assert_not_called()

    def test_duration_filter_rejects_too_long(self, tmp_path):
        """Videos longer than 600s should be skipped."""
        dl = self._make_downloader(tmp_path)
        dl.youtube.search.return_value.list.return_value.execute.return_value = {
            "items": [{"id": {"videoId": "vid2"}, "snippet": {"title": "Full Match"}}]
        }
        dl._get_video_duration = MagicMock(return_value=7200)
        dl._download_video = MagicMock()

        match = {"id": "m2", "home_team": "Aston Villa", "away_team": "Chelsea"}
        result = dl.download_match_highlights(match)

        assert result is False
        dl._download_video.assert_not_called()

    def test_valid_highlight_is_downloaded(self, tmp_path):
        """A 180-second highlight clip should be downloaded."""
        dl = self._make_downloader(tmp_path)
        dl.youtube.search.return_value.list.return_value.execute.return_value = {
            "items": [{"id": {"videoId": "vid3"}, "snippet": {"title": "Villa vs Liverpool Highlights"}}]
        }
        dl._get_video_duration = MagicMock(return_value=180)
        # Simulate download creating the tmp file, then trimming
        tmp_file = tmp_path / "video" / "m3_highlights_full.mp4"

        def fake_download(vid_id, dest):
            dest.write_bytes(b"fake_video_data")
            return True

        dl._download_video = MagicMock(side_effect=fake_download)
        dl._trim_video = MagicMock()

        match = {"id": "m3", "home_team": {"name": "Aston Villa"}, "away_team": {"name": "Liverpool"}}
        result = dl.download_match_highlights(match)

        assert result is True

    def test_missing_team_names_returns_false(self, tmp_path):
        dl = self._make_downloader(tmp_path)
        match = {"id": "m4"}  # No team fields
        result = dl.download_match_highlights(match)
        assert result is False

    def test_checkpoint_skips_already_downloaded(self, tmp_path):
        """Matches already in checkpoint should be skipped."""
        import pandas as pd
        dl = self._make_downloader(tmp_path)
        dl.checkpoint.is_completed.return_value = True
        dl._search_highlights = MagicMock()

        matches_csv = tmp_path / "matches.csv"
        pd.DataFrame([{"id": "existing_match", "home_team": "Liverpool", "away_team": "Villa"}]).to_csv(matches_csv, index=False)
        dl.run(matches_csv)

        dl._search_highlights.assert_not_called()
