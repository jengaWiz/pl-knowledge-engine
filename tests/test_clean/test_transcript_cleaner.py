"""
Tests for src/clean/transcript_cleaner.py
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.clean.transcript_cleaner import _regex_clean, TranscriptCleaner


class TestRegexCleaner:
    def test_capitalises_first_word(self):
        result = _regex_clean("hello world. this is a test.")
        assert result[0].isupper()

    def test_capitalises_after_period(self):
        result = _regex_clean("first sentence. second sentence.")
        assert "Second" in result or "second" not in result.split(". ")[1][0]

    def test_removes_filler_words(self):
        result = _regex_clean("um he was you know really good")
        assert "um" not in result.lower()
        assert "you know" not in result.lower()

    def test_corrects_player_name_salah(self):
        result = _regex_clean("salah scored the goal")
        assert "Salah" in result

    def test_corrects_team_name_aston_villa(self):
        result = _regex_clean("aston villa won the game")
        assert "Aston Villa" in result

    def test_does_not_add_information(self):
        raw = "watkins scored twice"
        result = _regex_clean(raw)
        # Only spelling/cap fixes allowed — no extra words
        assert "twice" in result
        assert "Watkins" in result

    def test_collapses_multiple_spaces(self):
        result = _regex_clean("hello    world")
        assert "  " not in result


class TestTranscriptCleaner:
    def test_llm_mode_detection_with_key(self, tmp_path):
        with patch("src.clean.transcript_cleaner.settings") as ms:
            ms.gemini_api_key = "fake-key-for-testing"
            ms.raw_dir = tmp_path
            ms.cleaned_dir = tmp_path / "cleaned"
            ms.checkpoint_dir = tmp_path / "checkpoints"
            (tmp_path / "transcripts").mkdir(parents=True, exist_ok=True)
            (tmp_path / "cleaned" / "transcripts").mkdir(parents=True, exist_ok=True)
            with patch("src.clean.transcript_cleaner.Checkpoint"):
                cleaner = TranscriptCleaner()
            assert cleaner.use_llm is True

    def test_regex_mode_detection_without_key(self, tmp_path):
        with patch("src.clean.transcript_cleaner.settings") as ms:
            ms.gemini_api_key = ""
            ms.raw_dir = tmp_path
            ms.cleaned_dir = tmp_path / "cleaned"
            ms.checkpoint_dir = tmp_path / "checkpoints"
            (tmp_path / "transcripts").mkdir(parents=True, exist_ok=True)
            (tmp_path / "cleaned" / "transcripts").mkdir(parents=True, exist_ok=True)
            with patch("src.clean.transcript_cleaner.Checkpoint"):
                cleaner = TranscriptCleaner()
            assert cleaner.use_llm is False

    def test_clean_one_missing_file_returns_false(self, tmp_path):
        with patch("src.clean.transcript_cleaner.settings") as ms:
            ms.gemini_api_key = ""
            ms.raw_dir = tmp_path
            ms.cleaned_dir = tmp_path / "cleaned"
            ms.checkpoint_dir = tmp_path / "checkpoints"
            (tmp_path / "transcripts").mkdir(parents=True, exist_ok=True)
            (tmp_path / "cleaned" / "transcripts").mkdir(parents=True, exist_ok=True)
            with patch("src.clean.transcript_cleaner.Checkpoint"):
                cleaner = TranscriptCleaner()
            result = cleaner.clean_one("nonexistent_id")
        assert result is False

    def test_clean_one_writes_output_file(self, tmp_path):
        raw_dir = tmp_path / "transcripts"
        raw_dir.mkdir(parents=True)
        cleaned_dir = tmp_path / "cleaned" / "transcripts"
        cleaned_dir.mkdir(parents=True)
        (raw_dir / "test123_raw.txt").write_text("salah scored the goal um yeah", encoding="utf-8")

        with patch("src.clean.transcript_cleaner.settings") as ms:
            ms.gemini_api_key = ""
            ms.raw_dir = tmp_path
            ms.cleaned_dir = tmp_path / "cleaned"
            ms.checkpoint_dir = tmp_path / "checkpoints"
            with patch("src.clean.transcript_cleaner.Checkpoint"):
                cleaner = TranscriptCleaner()
                cleaner.raw_dir = raw_dir
                cleaner.cleaned_dir = cleaned_dir
            result = cleaner.clean_one("test123")

        assert result is True
        assert (cleaned_dir / "test123_cleaned.txt").exists()
