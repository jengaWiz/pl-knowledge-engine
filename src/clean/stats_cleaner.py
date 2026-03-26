"""
Stats cleaning pipeline.

Loads raw JSON and CSV files from ``data/raw/stats/``, applies standard
normalisation transforms, adds derived match/season metrics, and outputs
cleaned CSVs and JSON to ``data/cleaned/stats/``.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import settings
from config.teams import ASTON_VILLA, LIVERPOOL, FOCUS_TEAM_NAMES
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _to_snake_case(name: str) -> str:
    """Convert a column name string to snake_case.

    Args:
        name: Original column name string (e.g. ``HomeTeam``, ``home-team``).

    Returns:
        Snake-cased column name string (e.g. ``home_team``).
    """
    # Replace hyphens/spaces with underscores then handle CamelCase
    s = re.sub(r"[-\s]+", "_", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all DataFrame columns to snake_case.

    Args:
        df: Input DataFrame with arbitrary column names.

    Returns:
        New DataFrame with snake_case column names.
    """
    df = df.copy()
    df.columns = [_to_snake_case(c) for c in df.columns]
    return df


# ---------------------------------------------------------------------------
# Match cleaner
# ---------------------------------------------------------------------------


def load_raw_matches(
    villa_path: Path, liverpool_path: Path
) -> pd.DataFrame:
    """Load and combine raw match JSON files into one DataFrame.

    Args:
        villa_path: Path to ``matches_villa.json``.
        liverpool_path: Path to ``matches_liverpool.json``.

    Returns:
        Combined DataFrame of all focus-team matches (deduped).
    """
    frames = []
    for path in (villa_path, liverpool_path):
        if not path.exists():
            logger.warning("raw match file not found", path=str(path))
            continue
        with open(path, encoding="utf-8") as f:
            records = json.load(f)
        if records:
            frames.append(pd.json_normalize(records))

    if not frames:
        logger.error("no raw match data found")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    return combined


def clean_matches(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the full cleaning pipeline to a raw matches DataFrame.

    Steps:
    1. Normalize column names to snake_case.
    2. Parse date strings to datetime.
    3. Coerce numeric fields (scores) to int where possible.
    4. Remove duplicate matches (composite key: date + home_team + away_team).
    5. Add derived columns: result (W/L/D), points (3/1/0),
       cumulative_points, form (last-5 string) — computed per team.

    Args:
        df: Raw matches DataFrame (may contain duplicates from both team files).

    Returns:
        Cleaned and enriched DataFrame sorted by date.
    """
    if df.empty:
        return df

    df = normalize_columns(df)
    logger.info("normalized columns", columns=list(df.columns))

    # Parse dates
    date_col = next(
        (c for c in df.columns if "date" in c and "time" not in c), None
    )
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.rename(columns={date_col: "match_date"})

    # Identify home/away team and score columns
    home_team_col = next(
        (c for c in df.columns if "home" in c and "team" in c and "score" not in c), None
    )
    away_team_col = next(
        (c for c in df.columns if "away" in c and "team" in c and "score" not in c), None
    )
    home_score_col = next(
        (c for c in df.columns if "home" in c and "score" in c), None
    )
    away_score_col = next(
        (c for c in df.columns if "away" in c and "score" in c), None
    )

    # Coerce scores to nullable int
    for col in (home_score_col, away_score_col):
        if col:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Remove duplicates
    dedup_keys = [k for k in ("match_date", home_team_col, away_team_col) if k]
    if dedup_keys:
        before = len(df)
        df = df.drop_duplicates(subset=dedup_keys)
        logger.info("removed duplicates", before=before, after=len(df))

    # Sort chronologically
    if "match_date" in df.columns:
        df = df.sort_values("match_date").reset_index(drop=True)

    # Add derived columns for each focus team
    if all(c for c in (home_team_col, away_team_col, home_score_col, away_score_col)):
        df = _add_team_derived_columns(
            df,
            home_team_col=home_team_col,
            away_team_col=away_team_col,
            home_score_col=home_score_col,
            away_score_col=away_score_col,
        )

    return df


def _add_team_derived_columns(
    df: pd.DataFrame,
    home_team_col: str,
    away_team_col: str,
    home_score_col: str,
    away_score_col: str,
) -> pd.DataFrame:
    """Compute result, points, cumulative_points, and form for each focus team.

    Adds columns:
    - ``{team_slug}_result``: "W", "L", or "D" from the team's perspective.
    - ``{team_slug}_points``: 3, 1, or 0.
    - ``{team_slug}_cumulative_points``: running season total.
    - ``{team_slug}_form``: last-5-match result string (e.g., "WWDLW").

    Args:
        df: Cleaned matches DataFrame (sorted chronologically).
        home_team_col: Column name for home team identifier.
        away_team_col: Column name for away team identifier.
        home_score_col: Column name for home team score.
        away_score_col: Column name for away team score.

    Returns:
        DataFrame with added derived columns.
    """
    team_configs = {
        "villa": ASTON_VILLA.name,
        "liverpool": LIVERPOOL.name,
    }

    for slug, team_name in team_configs.items():
        team_lower = team_name.lower()

        def _result_for_team(row: pd.Series, tl: str = team_lower) -> str | None:
            home = str(row.get(home_team_col, "")).lower()
            away = str(row.get(away_team_col, "")).lower()
            hs = row.get(home_score_col)
            as_ = row.get(away_score_col)
            if pd.isna(hs) or pd.isna(as_):
                return None
            hs, as_ = int(hs), int(as_)
            if tl in home:
                if hs > as_:
                    return "W"
                if hs < as_:
                    return "L"
                return "D"
            if tl in away:
                if as_ > hs:
                    return "W"
                if as_ < hs:
                    return "L"
                return "D"
            return None  # Team not involved

        result_col = f"{slug}_result"
        points_col = f"{slug}_points"
        cum_col = f"{slug}_cumulative_points"
        form_col = f"{slug}_form"

        df[result_col] = df.apply(_result_for_team, axis=1)
        df[points_col] = df[result_col].map({"W": 3, "D": 1, "L": 0})

        # Cumulative points only where team was involved
        involved = df[result_col].notna()
        df.loc[involved, cum_col] = (
            df.loc[involved, points_col].cumsum()
        )

        # Form: last 5 results as a string (skip rows where team not involved)
        team_rows = df[involved].copy()
        form_values = []
        for i in range(len(team_rows)):
            window = team_rows.iloc[max(0, i - 4) : i + 1][result_col].tolist()
            form_values.append("".join(r for r in window if r))
        team_rows[form_col] = form_values
        df[form_col] = team_rows[form_col]

        logger.info("added derived columns", team=slug, involved_matches=involved.sum())

    return df


# ---------------------------------------------------------------------------
# Players / FPL cleaner
# ---------------------------------------------------------------------------


def clean_fpl_players(gw_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine and clean per-gameweek FPL player stat DataFrames.

    Args:
        gw_dfs: List of raw per-gameweek DataFrames (one per gameweek).

    Returns:
        Combined and normalized DataFrame of all gameweek records.
    """
    if not gw_dfs:
        return pd.DataFrame()

    combined = pd.concat(gw_dfs, ignore_index=True)
    combined = normalize_columns(combined)

    # Coerce numeric columns
    numeric_cols = [
        c for c in combined.columns
        if any(kw in c for kw in ("goals", "assists", "minutes", "points", "price", "gw"))
    ]
    for col in numeric_cols:
        combined[col] = pd.to_numeric(combined[col], errors="coerce").fillna(0)

    return combined


# ---------------------------------------------------------------------------
# Standings cleaner
# ---------------------------------------------------------------------------


def clean_standings(raw_path: Path) -> pd.DataFrame:
    """Load and clean raw standings JSON.

    Args:
        raw_path: Path to ``standings.json``.

    Returns:
        Normalized standings DataFrame.
    """
    if not raw_path.exists():
        logger.warning("standings file not found", path=str(raw_path))
        return pd.DataFrame()

    with open(raw_path, encoding="utf-8") as f:
        records = json.load(f)

    df = pd.json_normalize(records)
    df = normalize_columns(df)
    logger.info("loaded standings", rows=len(df))
    return df


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_stats_cleaning() -> dict[str, pd.DataFrame]:
    """Run the full stats cleaning pipeline.

    Loads raw data from ``data/raw/stats/``, cleans it, and writes output
    to ``data/cleaned/stats/``.

    Returns:
        Dictionary mapping output name to cleaned DataFrame.
    """
    raw_stats = settings.raw_dir / "stats"
    cleaned_stats = settings.cleaned_dir / "stats"
    cleaned_stats.mkdir(parents=True, exist_ok=True)

    # --- Matches ---
    raw_df = load_raw_matches(
        raw_stats / "matches_villa.json",
        raw_stats / "matches_liverpool.json",
    )
    matches_df = clean_matches(raw_df)
    if not matches_df.empty:
        matches_df.to_csv(cleaned_stats / "matches.csv", index=False)
        matches_df.to_json(cleaned_stats / "matches.json", orient="records", indent=2)
        logger.info("saved cleaned matches", rows=len(matches_df))

    # --- Standings ---
    standings_df = clean_standings(raw_stats / "standings.json")
    if not standings_df.empty:
        standings_df.to_csv(cleaned_stats / "standings_history.csv", index=False)
        logger.info("saved cleaned standings", rows=len(standings_df))

    # --- FPL per-team player files ---
    fpl_dir = raw_stats / "fpl"
    for team, slug in ((ASTON_VILLA.name, "villa"), (LIVERPOOL.name, "liverpool")):
        gw_frames = []
        for gw_dir in sorted(fpl_dir.glob("GW*")):
            stats_file = gw_dir / "playerstats_gw.csv"
            if stats_file.exists():
                gw_df = pd.read_csv(stats_file)
                team_col = next(
                    (c for c in gw_df.columns if c.lower() in ("team", "team_name", "club")), None
                )
                if team_col:
                    gw_frames.append(
                        gw_df[gw_df[team_col].str.lower() == team.lower()]
                    )

        if gw_frames:
            players_df = clean_fpl_players(gw_frames)
            players_df.to_csv(cleaned_stats / f"players_{slug}.csv", index=False)
            logger.info("saved cleaned players", team=slug, rows=len(players_df))

    logger.info("stats cleaning complete")
    return {
        "matches": matches_df,
        "standings": standings_df,
    }
