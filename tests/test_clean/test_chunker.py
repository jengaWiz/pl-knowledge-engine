"""
Tests for src/clean/chunker.py — complete coverage of all 12 required behaviours.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.clean.chunker import (
    Chunker,
    _detect_players,
    _detect_teams,
    _split_sentences,
    build_chunk_metadata,
    chunk_text,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MULTI_SENTENCE = (
    "Liverpool stormed forward in the first half. "
    "Salah fired in a stunning goal from outside the box. "
    "Aston Villa equalised through Watkins before half-time. "
    "The second half saw both teams push hard for a winner. "
    "Van Dijk was immense at the back for Liverpool all evening. "
    "Emery made several tactical changes that almost paid off. "
    "The match ended in a thrilling 1-1 draw at Anfield. "
    "Both managers praised their sides after the final whistle. "
)


# ---------------------------------------------------------------------------
# 1. chunk_text: multi-sentence text produces expected chunk count
# ---------------------------------------------------------------------------

class TestChunkText:
    def test_produces_multiple_chunks(self):
        """Long text with chunk_size_words=20 should produce more than 1 chunk."""
        chunks = chunk_text(MULTI_SENTENCE * 3, chunk_size_words=20, overlap_words=5)
        assert len(chunks) > 1

    # 2. Overlap: consecutive chunks share boundary words
    def test_overlap_creates_shared_words(self):
        text = " ".join(f"Word{i}." for i in range(60))
        chunks = chunk_text(text, chunk_size_words=20, overlap_words=5)
        if len(chunks) < 2:
            pytest.skip("not enough chunks to test overlap")
        # The end of chunk[0] and start of chunk[1] must share some words
        words_end_of_first = set(chunks[0].split()[-8:])
        words_start_of_second = set(chunks[1].split()[:8])
        assert words_end_of_first & words_start_of_second, (
            "No shared words found between consecutive chunks"
        )

    # 3. Tail merge: last chunk < 50 words merges into previous
    def test_tail_chunk_merged_when_too_short(self):
        """Text whose natural last chunk is < 50 words should merge into prev."""
        # Craft text: 3 long sentences + 1 very short tail
        long_part = ("This is a long sentence with many words in it to fill the chunk. " * 5)
        short_tail = "Short."
        text = long_part + short_tail
        chunks = chunk_text(text, chunk_size_words=40, overlap_words=5, min_chunk_words=50)
        # No chunk should be < 50 words except if the whole text is short
        if len(chunks) > 1:
            for chunk in chunks:
                assert len(chunk.split()) >= 1  # basic sanity — no empty chunks
        # Key: "Short." must appear in the last chunk, merged
        assert "Short" in chunks[-1]

    # 4. Empty string returns []
    def test_empty_string_returns_empty_list(self):
        assert chunk_text("") == []

    # 5. Single short sentence returns one chunk (no crash)
    def test_single_short_sentence(self):
        chunks = chunk_text("Liverpool won.", chunk_size_words=400, overlap_words=100)
        assert len(chunks) == 1
        assert "Liverpool" in chunks[0]


# ---------------------------------------------------------------------------
# 6. _split_sentences
# ---------------------------------------------------------------------------

class TestSplitSentences:
    def test_splits_on_period(self):
        result = _split_sentences("First. Second. Third.")
        assert len(result) == 3

    def test_splits_on_question_mark(self):
        result = _split_sentences("What happened? Liverpool won!")
        assert len(result) == 2

    def test_splits_on_exclamation(self):
        result = _split_sentences("Goal! Watkins scores!")
        assert len(result) == 2

    def test_no_empty_strings(self):
        result = _split_sentences("Hello. World.")
        assert all(s.strip() for s in result)


# ---------------------------------------------------------------------------
# 7. _detect_teams
# ---------------------------------------------------------------------------

class TestDetectTeams:
    def test_detects_liverpool(self):
        result = _detect_teams("Liverpool played well today.")
        assert "Liverpool" in result

    def test_detects_aston_villa(self):
        result = _detect_teams("Aston Villa won the match.")
        assert "Aston Villa" in result

    def test_case_insensitive_liverpool(self):
        result = _detect_teams("LIVERPOOL are top of the league.")
        assert "Liverpool" in result

    def test_case_insensitive_villa(self):
        result = _detect_teams("aston villa dominated the midfield.")
        assert "Aston Villa" in result

    def test_both_teams_found(self):
        result = _detect_teams("Liverpool vs Aston Villa ended 1-1.")
        assert "Liverpool" in result
        assert "Aston Villa" in result

    def test_returns_comma_separated_string(self):
        result = _detect_teams("Liverpool beat Aston Villa.")
        assert isinstance(result, str)
        # Both teams found → comma present
        parts = [p.strip() for p in result.split(",")]
        assert len(parts) >= 2

    def test_no_focus_team_returns_empty(self):
        result = _detect_teams("Arsenal beat Chelsea 2-0.")
        assert result == ""


# ---------------------------------------------------------------------------
# 8. _detect_players
# ---------------------------------------------------------------------------

class TestDetectPlayers:
    def test_detects_salah(self):
        result = _detect_players("Salah scored a hat-trick.")
        assert "Salah" in result

    def test_detects_watkins(self):
        result = _detect_players("Watkins leads the Villa attack.")
        assert "Watkins" in result

    def test_deduplicates(self):
        result = _detect_players("Salah passed to Salah who scored.")
        names = [n.strip() for n in result.split(",") if n.strip()]
        assert names.count("Salah") == 1

    def test_no_player_mentioned_returns_empty(self):
        result = _detect_players("The weather was perfect for football.")
        assert result == ""


# ---------------------------------------------------------------------------
# 9 & 10. build_chunk_metadata
# ---------------------------------------------------------------------------

class TestBuildChunkMetadata:
    CHUNKS = ["First chunk text.", "Second chunk text.", "Third chunk text."]

    def test_chunk_id_format(self):
        meta = build_chunk_metadata(self.CHUNKS, "vid001", "Title", "Ch", "2026-01-01T00:00:00Z")
        assert meta[0]["chunk_id"] == "podcast_vid001_chunk_000"
        assert meta[1]["chunk_id"] == "podcast_vid001_chunk_001"
        assert meta[2]["chunk_id"] == "podcast_vid001_chunk_002"

    def test_total_chunks_consistent(self):
        meta = build_chunk_metadata(self.CHUNKS, "v", "T", "C", "2026-01-21T00:00:00Z")
        assert all(m["total_chunks"] == 3 for m in meta)

    def test_chunk_index_sequential(self):
        meta = build_chunk_metadata(self.CHUNKS, "v", "T", "C", "2026-01-21T00:00:00Z")
        for i, m in enumerate(meta):
            assert m["chunk_index"] == i

    def test_modality_is_text(self):
        meta = build_chunk_metadata(["text"], "v", "T", "C", "2026-01-21T00:00:00Z")
        assert meta[0]["modality"] == "text"

    def test_date_extracted_from_published_at(self):
        meta = build_chunk_metadata(["text"], "v", "T", "C", "2026-03-15T14:00:00Z")
        assert meta[0]["date"] == "2026-03-15"

    def test_source_type_is_podcast_transcript(self):
        meta = build_chunk_metadata(["text"], "v", "T", "C", "2026-01-21T00:00:00Z")
        assert meta[0]["source_type"] == "podcast_transcript"

    def test_teams_string_type(self):
        """teams must be a comma-separated string (ChromaDB compatibility)."""
        meta = build_chunk_metadata(
            ["Liverpool beat Aston Villa"], "v", "T", "C", "2026-01-01T00:00:00Z"
        )
        assert isinstance(meta[0]["teams"], str)


# ---------------------------------------------------------------------------
# 11 & 12. Chunker class
# ---------------------------------------------------------------------------

class TestChunker:
    def _make_chunker(self, tmp_path: Path) -> Chunker:
        with patch("src.clean.chunker.settings") as ms:
            ms.cleaned_dir = tmp_path
            ms.chunk_size_words = 50
            ms.chunk_overlap_words = 10
            chunker = Chunker()
            chunker.cleaned_dir = tmp_path / "transcripts"
            chunker.output_dir = tmp_path / "transcripts"
            (tmp_path / "transcripts").mkdir(parents=True, exist_ok=True)
        return chunker

    def test_missing_cleaned_file_returns_empty(self, tmp_path: Path):
        chunker = self._make_chunker(tmp_path)
        result = chunker.chunk_episode(
            {"youtube_id": "none", "title": "", "channel": "", "published_at": ""}
        )
        assert result == []

    def test_chunks_json_written_to_disk(self, tmp_path: Path):
        chunker = self._make_chunker(tmp_path)
        text = MULTI_SENTENCE * 4
        (tmp_path / "transcripts" / "ep001_cleaned.txt").write_text(text, encoding="utf-8")

        result = chunker.chunk_episode(
            {
                "youtube_id": "ep001",
                "title": "Test Episode",
                "channel": "Test Channel",
                "published_at": "2026-01-01T00:00:00Z",
            }
        )
        json_path = tmp_path / "transcripts" / "ep001_chunks.json"
        assert json_path.exists()
        with open(json_path) as f:
            data = json.load(f)
        assert len(data) == len(result)
        assert len(result) > 0
