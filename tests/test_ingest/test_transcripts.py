"""
Tests for src/ingest/transcripts.py
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.ingest.transcripts import TranscriptFetcher


def _make_fetcher(tmp_path):
    with patch("src.ingest.transcripts.settings") as ms, \
         patch("src.ingest.transcripts.YouTubeTranscriptApi"):
        ms.raw_dir = tmp_path
        ms.checkpoint_dir = tmp_path / "checkpoints"
        (tmp_path / "transcripts").mkdir(parents=True, exist_ok=True)
        fetcher = TranscriptFetcher.__new__(TranscriptFetcher)
        fetcher.api = MagicMock()
        fetcher.transcript_dir = tmp_path / "transcripts"
        fetcher.checkpoint = MagicMock()
        fetcher.checkpoint.is_completed.return_value = False
        return fetcher


class TestTranscriptFetcher:
    def test_saves_raw_text_and_segments(self, tmp_path):
        """Fetching a valid transcript should write both output files."""
        fetcher = _make_fetcher(tmp_path)
        segments = [
            {"text": "hello world", "start": 0.0, "duration": 2.5},
            {"text": "this is a test", "start": 2.5, "duration": 3.0},
        ]
        mock_transcript = MagicMock()
        mock_transcript.fetch.return_value = segments
        fetcher.api.list_transcripts.return_value.find_manually_created_transcript.return_value = mock_transcript

        fetcher._save_transcript("vid123", segments)

        raw_path = tmp_path / "transcripts" / "vid123_raw.txt"
        seg_path = tmp_path / "transcripts" / "vid123_segments.json"
        assert raw_path.exists()
        assert seg_path.exists()
        assert "hello world" in raw_path.read_text()

    def test_raw_text_concatenates_segments(self, tmp_path):
        """Raw text should be all segment texts joined by spaces."""
        fetcher = _make_fetcher(tmp_path)
        segments = [{"text": "first", "start": 0.0, "duration": 1.0},
                    {"text": "second", "start": 1.0, "duration": 1.0}]
        fetcher._save_transcript("xyz", segments)
        raw = (tmp_path / "transcripts" / "xyz_raw.txt").read_text()
        assert raw == "first second"

    def test_transcripts_disabled_returns_none(self, tmp_path):
        """TranscriptsDisabled exception should return None gracefully."""
        from youtube_transcript_api import TranscriptsDisabled
        fetcher = _make_fetcher(tmp_path)
        fetcher.api.list_transcripts.side_effect = TranscriptsDisabled("vid999")
        result = fetcher._fetch_transcript("vid999")
        assert result is None

    def test_no_transcript_found_returns_none(self, tmp_path):
        """NoTranscriptFound exception should return None gracefully."""
        from youtube_transcript_api import NoTranscriptFound
        fetcher = _make_fetcher(tmp_path)
        fetcher.api.list_transcripts.return_value.find_manually_created_transcript.side_effect = NoTranscriptFound("vid999", [], [])
        fetcher.api.list_transcripts.return_value.find_generated_transcript.side_effect = NoTranscriptFound("vid999", [], [])
        result = fetcher._fetch_transcript("vid999")
        assert result is None

    def test_checkpoint_skips_already_fetched(self, tmp_path):
        """Episodes marked as completed in checkpoint should be skipped."""
        fetcher = _make_fetcher(tmp_path)
        fetcher.checkpoint.is_completed.return_value = True
        episodes = [{"youtube_id": "already_done"}]
        results = fetcher.fetch_all(episodes)
        assert results["already_done"] is True
        fetcher.api.list_transcripts.assert_not_called()
