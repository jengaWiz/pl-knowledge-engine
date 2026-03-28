"""
Tests for src/clean/stats_summarizer.py
"""
from __future__ import annotations

import json
import pandas as pd
import pytest

from src.clean.stats_summarizer import (
    _summarize_match,
    _summarize_player_row,
    summarize_matches,
    summarize_players,
)


class TestSummarizeMatch:
    def _row(self, **kwargs) -> pd.Series:
        defaults = {
            "home_team_name": "Liverpool",
            "away_team_name": "Aston Villa",
            "home_score": 1,
            "away_score": 2,
            "match_date": "2026-01-21",
            "venue": "Anfield",
            "gameweek": 22,
            "id": "match_001",
        }
        defaults.update(kwargs)
        return pd.Series(defaults)

    def test_returns_dict_with_required_keys(self):
        result = _summarize_match(self._row())
        assert result is not None
        for key in ("chunk_id", "text", "source_type", "date", "modality", "teams"):
            assert key in result

    def test_text_contains_team_names(self):
        result = _summarize_match(self._row())
        assert "Liverpool" in result["text"]
        assert "Aston Villa" in result["text"]

    def test_text_contains_score(self):
        result = _summarize_match(self._row())
        assert "1" in result["text"] and "2" in result["text"]

    def test_away_win_narrative(self):
        result = _summarize_match(self._row(home_score=1, away_score=2))
        assert "away" in result["text"].lower()

    def test_home_win_narrative(self):
        result = _summarize_match(self._row(home_score=3, away_score=0))
        assert "home" in result["text"].lower()

    def test_draw_narrative(self):
        result = _summarize_match(self._row(home_score=1, away_score=1))
        assert "draw" in result["text"].lower()

    def test_missing_teams_returns_none(self):
        result = _summarize_match(pd.Series({"home_score": 1, "away_score": 2}))
        assert result is None

    def test_modality_is_text(self):
        result = _summarize_match(self._row())
        assert result["modality"] == "text"

    def test_source_type_is_match_stats(self):
        result = _summarize_match(self._row())
        assert result["source_type"] == "match_stats"

    def test_date_extracted_correctly(self):
        result = _summarize_match(self._row(match_date="2026-03-15T14:00:00Z"))
        assert result["date"] == "2026-03-15"


class TestSummarizePlayerRow:
    def _row(self, **kwargs) -> pd.Series:
        defaults = {
            "name": "Mohamed Salah",
            "team": "Liverpool",
            "gw": 22,
            "goals_scored": 1,
            "assists": 0,
            "minutes": 87,
            "total_points": 9,
        }
        defaults.update(kwargs)
        return pd.Series(defaults)

    def test_returns_dict_with_required_keys(self):
        result = _summarize_player_row(self._row())
        assert result is not None
        for key in ("chunk_id", "text", "players", "teams", "gameweek"):
            assert key in result

    def test_text_contains_player_name(self):
        result = _summarize_player_row(self._row())
        assert "Mohamed Salah" in result["text"]

    def test_text_contains_goals(self):
        result = _summarize_player_row(self._row(goals_scored=2))
        assert "2 goal" in result["text"]

    def test_text_singular_goal(self):
        result = _summarize_player_row(self._row(goals_scored=1))
        assert "1 goal" in result["text"]
        assert "goals" not in result["text"]

    def test_missing_name_returns_none(self):
        result = _summarize_player_row(pd.Series({"gw": 1, "goals_scored": 1}))
        assert result is None

    def test_players_field_set(self):
        result = _summarize_player_row(self._row(name="Ollie Watkins"))
        assert result["players"] == "Ollie Watkins"


class TestSummarizeMatchesFile:
    def test_missing_file_returns_empty(self, tmp_path):
        result = summarize_matches(tmp_path / "nonexistent.csv")
        assert result == []

    def test_loads_csv_and_returns_summaries(self, tmp_path):
        df = pd.DataFrame([{
            "home_team_name": "Liverpool", "away_team_name": "Aston Villa",
            "home_score": 1, "away_score": 2,
            "match_date": "2026-01-21", "venue": "Anfield",
            "gameweek": 22, "id": "m1",
        }])
        path = tmp_path / "matches.csv"
        df.to_csv(path, index=False)
        result = summarize_matches(path)
        assert len(result) == 1
        assert "Liverpool" in result[0]["text"]


class TestSummarizePlayersFiles:
    def test_missing_files_returns_empty(self, tmp_path):
        result = summarize_players(tmp_path)
        assert result == []

    def test_loads_both_team_files(self, tmp_path):
        for slug in ("villa", "liverpool"):
            df = pd.DataFrame([{
                "name": "Player A", "team": slug,
                "gw": 1, "goals_scored": 1, "assists": 0, "minutes": 90,
            }])
            df.to_csv(tmp_path / f"players_{slug}.csv", index=False)

        result = summarize_players(tmp_path)
        assert len(result) == 2
