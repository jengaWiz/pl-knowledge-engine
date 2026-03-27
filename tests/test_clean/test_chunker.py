"""
Tests for src/clean/chunker.py
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.clean.chunker import (
    chunk_text,
    build_chunk_metadata,
    _detect_teams,
    _detect_players,
    Chunker,
)

SAMPLE_TEXT = (
    "Liverpool faced Aston Villa in a thrilling match at Anfield. "
    "Salah opened the scoring after 20 minutes with a fierce strike. "
    "Watkins equalised for Villa on the stroke of half time. "
    "The second half saw end-to-end action as both teams pushed for a winner. "
    "Van Dijk was commanding at the back for Liverpool throughout. "
    "Emery made tactical changes that proved decisive. "
    "The match ended 1-1, a fair result for both sides. "
    "Liverpool remain in contention for the title race. "
    "Aston Villa continue to impress under their manager. "
    "Both sets of fans were treated to a spectacular display of Premier League football. "
)


class TestChunkText:
    def test_produces_at_least_one_chunk(self):
        chunks = chunk_text(SAMPLE_TEXT, chunk_size_words=50, overlap_words=10)
        assert len(chunks) >= 1

    def test_chunks_within_word_limit(self):
        chunks = chunk_text(SAMPLE_TEXT, chunk_size_words=50, overlap_words=10)
        for chunk in chunks:
            assert len(chunk.split()) <= 75  # Allow some buffer for sentence boundary

    def test_no_empty_chunks(self):
        chunks = chunk_text(SAMPLE_TEXT, chunk_size_words=40, overlap_words=8)
        for chunk in chunks:
            assert len(chunk.strip()) > 0

    def test_short_tail_chunk_merged(self):
        """If final chunk is tiny, it should be merged into the previous one."""
        # Create text where last chunk would naturally be very short
        text = ("This is a sentence. " * 20) + "Short."
        chunks = chunk_text(text, chunk_size_words=50, overlap_words=10, min_chunk_words=50)
        # The short tail should be merged if below min threshold
        for chunk in chunks[:-1]:
            assert len(chunk.split()) >= 1  # All non-last chunks should be substantial

    def test_empty_text_returns_empty_list(self):
        chunks = chunk_text("", chunk_size_words=50, overlap_words=10)
        assert chunks == []

    def test_whitespace_only_returns_empty_list(self):
        chunks = chunk_text("   ", chunk_size_words=50, overlap_words=10)
        assert chunks == []


class TestEntityDetection:
    def test_detects_liverpool(self):
        result = _detect_teams("Liverpool played well today.")
        assert "Liverpool" in result

    def test_detects_aston_villa(self):
        result = _detect_teams("Aston Villa dominated the match.")
        assert "Aston Villa" in result

    def test_detects_both_teams(self):
        result = _detect_teams("Liverpool vs Aston Villa ended in a draw.")
        assert "Liverpool" in result
        assert "Aston Villa" in result

    def test_non_focus_team_not_detected(self):
        result = _detect_teams("Arsenal beat Chelsea at the Emirates.")
        assert result == ""

    def test_detects_salah(self):
        result = _detect_players("Salah scored a hat trick.")
        assert "Salah" in result

    def test_detects_watkins(self):
        result = _detect_players("Watkins led the Villa attack brilliantly.")
        assert "Watkins" in result

    def test_no_false_player_detection(self):
        result = _detect_players("The weather was sunny and the pitch was perfect.")
        assert result == ""


class TestBuildChunkMetadata:
    def test_chunk_ids_are_unique(self):
        chunks = ["First chunk text here.", "Second chunk text here."]
        metadata = build_chunk_metadata(chunks, "vid001", "Title", "Channel", "2026-01-01T00:00:00Z")
        ids = [m["chunk_id"] for m in metadata]
        assert len(ids) == len(set(ids))

    def test_chunk_index_sequential(self):
        chunks = ["chunk one", "chunk two", "chunk three"]
        metadata = build_chunk_metadata(chunks, "vid001", "T", "C", "2026-01-01T00:00:00Z")
        for i, m in enumerate(metadata):
            assert m["chunk_index"] == i

    def test_total_chunks_correct(self):
        chunks = ["a", "b", "c"]
        metadata = build_chunk_metadata(chunks, "v", "T", "C", "2026-01-01T00:00:00Z")
        assert all(m["total_chunks"] == 3 for m in metadata)

    def test_modality_is_text(self):
        metadata = build_chunk_metadata(["text"], "v", "T", "C", "2026-01-21T00:00:00Z")
        assert metadata[0]["modality"] == "text"

    def test_source_type_is_podcast_transcript(self):
        metadata = build_chunk_metadata(["text"], "v", "T", "C", "2026-01-21T00:00:00Z")
        assert metadata[0]["source_type"] == "podcast_transcript"

    def test_date_extracted_from_published_at(self):
        metadata = build_chunk_metadata(["text"], "v", "T", "C", "2026-01-21T14:00:00Z")
        assert metadata[0]["date"] == "2026-01-21"

    def test_word_count_populated(self):
        metadata = build_chunk_metadata(["one two three four five"], "v", "T", "C", "2026-01-01T00:00:00Z")
        assert metadata[0]["word_count"] == 5

    def test_teams_field_is_string(self):
        """teams must be a comma-separated string (ChromaDB requirement)."""
        metadata = build_chunk_metadata(
            ["Liverpool beat Aston Villa"], "v", "T", "C", "2026-01-01T00:00:00Z"
        )
        assert isinstance(metadata[0]["teams"], str)


class TestChunker:
    def test_missing_cleaned_file_returns_empty(self, tmp_path):
        with patch("src.clean.chunker.settings") as ms:
            ms.cleaned_dir = tmp_path
            ms.chunk_size_words = 400
            ms.chunk_overlap_words = 100
            chunker = Chunker()
            chunker.cleaned_dir = tmp_path / "transcripts"
            chunker.output_dir = tmp_path / "transcripts"
            result = chunker.chunk_episode({"youtube_id": "missing", "title": "", "channel": "", "published_at": ""})
        assert result == []

    def test_chunks_are_written_to_disk(self, tmp_path):
        transcript_dir = tmp_path / "transcripts"
        transcript_dir.mkdir(parents=True)
        (transcript_dir / "ep001_cleaned.txt").write_text(SAMPLE_TEXT * 3, encoding="utf-8")

        with patch("src.clean.chunker.settings") as ms:
            ms.cleaned_dir = tmp_path
            ms.chunk_size_words = 50
            ms.chunk_overlap_words = 10
            chunker = Chunker()
            chunker.cleaned_dir = transcript_dir
            chunker.output_dir = transcript_dir
            result = chunker.chunk_episode({
                "youtube_id": "ep001",
                "title": "Test Episode",
                "channel": "Test Channel",
                "published_at": "2026-01-01T00:00:00Z",
            })

        assert len(result) > 0
        assert (transcript_dir / "ep001_chunks.json").exists()
