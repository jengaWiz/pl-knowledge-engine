"""
Tests for src/clean/audio_segmenter.py
"""
from __future__ import annotations

import json
import shutil
from unittest.mock import MagicMock, patch

import pytest


FFMPEG_AVAILABLE = bool(shutil.which("ffmpeg"))
requires_ffmpeg = pytest.mark.skipif(
    not FFMPEG_AVAILABLE,
    reason="ffmpeg not installed on this machine",
)


def _make_segmenter(tmp_path):
    """Build an AudioSegmenter with paths redirected to tmp_path."""
    with patch("src.clean.audio_segmenter._check_ffmpeg"), \
         patch("src.clean.audio_segmenter.settings") as ms, \
         patch("src.clean.audio_segmenter.Checkpoint"):
        ms.raw_dir = tmp_path
        ms.cleaned_dir = tmp_path / "cleaned"
        ms.audio_segment_seconds = 5
        ms.audio_overlap_seconds = 1
        ms.checkpoint_dir = tmp_path / "checkpoints"
        from src.clean.audio_segmenter import AudioSegmenter
        seg = AudioSegmenter.__new__(AudioSegmenter)
        seg.audio_dir = tmp_path / "audio"
        seg.segments_dir = tmp_path / "cleaned" / "audio_segments"
        seg.segments_dir.mkdir(parents=True, exist_ok=True)
        seg.segment_ms = 5000
        seg.overlap_ms = 1000
        seg.checkpoint = MagicMock()
        seg.checkpoint.is_completed.return_value = False
    return seg


class TestAudioSegmenter:
    def test_missing_mp3_returns_empty(self, tmp_path):
        """segment_one() should return [] if the source MP3 doesn't exist."""
        seg = _make_segmenter(tmp_path)
        result = seg.segment_one(
            {"youtube_id": "missing", "title": "", "channel": "", "published_at": ""}
        )
        assert result == []

    @requires_ffmpeg
    def test_segment_count_matches_duration(self, tmp_path):
        """A 15-second audio with 5s segments and 1s overlap yields ≥3 segments."""
        from pydub import AudioSegment as PyAudio

        seg = _make_segmenter(tmp_path)
        audio_dir = tmp_path / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)

        silence = PyAudio.silent(duration=15000)
        mp3_path = audio_dir / "test_vid.mp3"
        silence.export(str(mp3_path), format="mp3")

        episode = {
            "youtube_id": "test_vid",
            "title": "Test",
            "channel": "Ch",
            "published_at": "2026-01-01T00:00:00Z",
        }
        result = seg.segment_one(episode)
        assert len(result) >= 3

    @requires_ffmpeg
    def test_metadata_schema_has_required_fields(self, tmp_path):
        """Each segment metadata dict must have all Appendix A required fields."""
        from pydub import AudioSegment as PyAudio

        seg = _make_segmenter(tmp_path)
        audio_dir = tmp_path / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)

        silence = PyAudio.silent(duration=10000)
        silence.export(str(audio_dir / "vid99.mp3"), format="mp3")

        episode = {
            "youtube_id": "vid99",
            "title": "T",
            "channel": "C",
            "published_at": "2026-01-15T00:00:00Z",
        }
        result = seg.segment_one(episode)

        required_fields = {
            "segment_id", "source_type", "source_name", "source_id",
            "date", "modality", "chunk_index", "total_chunks",
            "start_seconds", "end_seconds", "duration_seconds",
        }
        for m in result:
            assert required_fields.issubset(m.keys()), f"Missing fields: {set(required_fields) - m.keys()}"

    @requires_ffmpeg
    def test_metadata_json_written_to_disk(self, tmp_path):
        """Segment metadata JSON should be saved alongside the MP3 files."""
        from pydub import AudioSegment as PyAudio

        seg = _make_segmenter(tmp_path)
        audio_dir = tmp_path / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)

        silence = PyAudio.silent(duration=8000)
        silence.export(str(audio_dir / "vid88.mp3"), format="mp3")

        episode = {
            "youtube_id": "vid88",
            "title": "T",
            "channel": "C",
            "published_at": "2026-02-01T00:00:00Z",
        }
        seg.segment_one(episode)

        meta_path = tmp_path / "cleaned" / "audio_segments" / "vid88_segments_meta.json"
        assert meta_path.exists()
        with open(meta_path) as f:
            data = json.load(f)
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_checkpoint_skips_already_segmented(self, tmp_path):
        """Episodes already in the checkpoint should be skipped."""
        seg = _make_segmenter(tmp_path)
        seg.checkpoint.is_completed.return_value = True

        episodes = [
            {"youtube_id": "already_done", "title": "", "channel": "", "published_at": ""}
        ]
        result = seg.segment_all(episodes)
        assert result == []
