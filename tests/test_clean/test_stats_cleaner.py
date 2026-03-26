"""
Tests for the stats cleaning pipeline (src/clean/stats_cleaner.py).
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.clean.stats_cleaner import (
    normalize_columns,
    clean_matches,
    load_raw_matches,
    _to_snake_case,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_raw_matches() -> list[dict]:
    """Minimal match records that mirror the BallDontLie API shape."""
    return [
        {
            "id": 1,
            "date": "2025-10-05",
            "home_team": {"name": "Aston Villa"},
            "away_team": {"name": "Liverpool"},
            "home_score": 2,
            "away_score": 1,
        },
        {
            "id": 2,
            "date": "2025-11-20",
            "home_team": {"name": "Liverpool"},
            "away_team": {"name": "Aston Villa"},
            "home_score": 0,
            "away_score": 0,
        },
        {
            "id": 3,
            "date": "2026-01-15",
            "home_team": {"name": "Aston Villa"},
            "away_team": {"name": "Arsenal"},
            "home_score": 1,
            "away_score": 2,
        },
    ]


@pytest.fixture
def sample_raw_df(sample_raw_matches) -> pd.DataFrame:
    """Normalized DataFrame from sample match records."""
    return pd.json_normalize(sample_raw_matches)


@pytest.fixture
def raw_match_files(tmp_path, sample_raw_matches) -> tuple[Path, Path]:
    """Write sample match records to temp JSON files."""
    villa_matches = [m for m in sample_raw_matches if "Aston Villa" in (m["home_team"]["name"], m["away_team"]["name"])]
    liverpool_matches = [m for m in sample_raw_matches if "Liverpool" in (m["home_team"]["name"], m["away_team"]["name"])]

    villa_path = tmp_path / "matches_villa.json"
    liverpool_path = tmp_path / "matches_liverpool.json"
    villa_path.write_text(json.dumps(villa_matches))
    liverpool_path.write_text(json.dumps(liverpool_matches))
    return villa_path, liverpool_path


# ---------------------------------------------------------------------------
# Column normalization tests
# ---------------------------------------------------------------------------


class TestColumnNormalization:
    """Tests for the snake_case column name normalizer."""

    def test_snake_case_lower(self):
        """Lower-case words separated by spaces become underscored."""
        assert _to_snake_case("home team") == "home_team"

    def test_snake_case_camel(self):
        """CamelCase strings are split at uppercase boundaries."""
        assert _to_snake_case("HomeScore") == "home_score"

    def test_snake_case_hyphen(self):
        """Hyphens are converted to underscores."""
        assert _to_snake_case("away-team") == "away_team"

    def test_snake_case_already_snake(self):
        """Already-snake_case strings pass through unchanged."""
        assert _to_snake_case("match_date") == "match_date"

    def test_normalize_columns_renames_all(self):
        """normalize_columns should rename all columns of a DataFrame."""
        df = pd.DataFrame(columns=["HomeTeam", "AwayTeam", "HomeScore", "AwayScore"])
        result = normalize_columns(df)
        assert list(result.columns) == ["home_team", "away_team", "home_score", "away_score"]

    def test_normalize_columns_preserves_data(self):
        """normalize_columns should not alter row data."""
        df = pd.DataFrame({"HomeTeam": ["Arsenal"], "GoalsFor": [2]})
        result = normalize_columns(df)
        assert result["home_team"].iloc[0] == "Arsenal"
        assert result["goals_for"].iloc[0] == 2


# ---------------------------------------------------------------------------
# Duplicate removal tests
# ---------------------------------------------------------------------------


class TestDuplicateRemoval:
    """Tests for composite-key deduplication."""

    def test_removes_duplicate_matches(self, raw_match_files, tmp_path):
        """Matches appearing in both villa and liverpool files should appear once."""
        villa_path, liverpool_path = raw_match_files
        raw_df = load_raw_matches(villa_path, liverpool_path)
        cleaned = clean_matches(raw_df)

        # Match 1 (AVL vs LFC) appears in both files — should only appear once
        # Match 2 (LFC vs AVL) appears in both files — same
        assert len(cleaned) == len(cleaned.drop_duplicates(subset=["id"]))

    def test_no_false_deduplication(self, raw_match_files):
        """Distinct matches (different dates/teams) should all be preserved."""
        villa_path, liverpool_path = raw_match_files
        raw_df = load_raw_matches(villa_path, liverpool_path)
        cleaned = clean_matches(raw_df)
        # We expect 3 unique matches total
        assert len(cleaned) == 3


# ---------------------------------------------------------------------------
# Derived field tests
# ---------------------------------------------------------------------------


class TestDerivedFields:
    """Tests for result, points, cumulative_points, and form calculations."""

    def test_villa_wins_home(self, sample_raw_df):
        """When Villa scores more at home, their result should be 'W'."""
        cleaned = clean_matches(sample_raw_df)
        # Match id=1: Villa 2-1 Liverpool → Villa W
        match1 = cleaned[cleaned["id"] == 1]
        assert not match1.empty
        assert match1["villa_result"].iloc[0] == "W"

    def test_liverpool_loses_away(self, sample_raw_df):
        """When Liverpool concedes more away, their result should be 'L'."""
        cleaned = clean_matches(sample_raw_df)
        match1 = cleaned[cleaned["id"] == 1]
        assert match1["liverpool_result"].iloc[0] == "L"

    def test_draw_registered_for_both_teams(self, sample_raw_df):
        """A 0-0 draw should register 'D' for both teams."""
        cleaned = clean_matches(sample_raw_df)
        match2 = cleaned[cleaned["id"] == 2]
        assert match2["villa_result"].iloc[0] == "D"
        assert match2["liverpool_result"].iloc[0] == "D"

    def test_points_mapping(self, sample_raw_df):
        """W=3, D=1, L=0 mapping should hold for all records."""
        cleaned = clean_matches(sample_raw_df)
        for _, row in cleaned.iterrows():
            result = row.get("villa_result")
            points = row.get("villa_points")
            if result == "W":
                assert points == 3
            elif result == "D":
                assert points == 1
            elif result == "L":
                assert points == 0

    def test_cumulative_points_increases(self, sample_raw_df):
        """Cumulative points should never decrease over time for a given team."""
        cleaned = clean_matches(sample_raw_df)
        villa_rows = cleaned[cleaned["villa_result"].notna()].sort_values("match_date")
        cum_pts = villa_rows["villa_cumulative_points"].dropna().tolist()
        for i in range(1, len(cum_pts)):
            assert cum_pts[i] >= cum_pts[i - 1]

    def test_form_string_builds_correctly(self, sample_raw_df):
        """Form string should be a concatenation of 'W', 'D', 'L' characters."""
        cleaned = clean_matches(sample_raw_df)
        villa_rows = cleaned[cleaned["villa_result"].notna()].sort_values("match_date")
        form_values = villa_rows["villa_form"].dropna().tolist()
        for form in form_values:
            assert all(c in "WDL" for c in form)
            assert len(form) <= 5

    def test_non_focus_team_match_has_none_result(self, sample_raw_df):
        """Matches not involving a focus team should have None for that team's result."""
        cleaned = clean_matches(sample_raw_df)
        # Match id=3: Arsenal vs Aston Villa — Liverpool not involved
        match3 = cleaned[cleaned["id"] == 3]
        assert pd.isna(match3["liverpool_result"].iloc[0])
