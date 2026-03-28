"""
Stats text summarizer.

Converts cleaned match and player stats DataFrames into natural-language
prose summaries suitable for embedding. Embeddings work far better on prose
than raw numbers, so every stat record becomes a short paragraph before it
enters the vector DB.

Output:
    data/cleaned/stats/match_summaries.json
    data/cleaned/stats/player_summaries.json
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Match summary
# ---------------------------------------------------------------------------


def _summarize_match(row: pd.Series) -> dict[str, Any] | None:
    """Build a natural-language summary for a single match row.

    Args:
        row: A row from the cleaned matches DataFrame.

    Returns:
        Dict with ``chunk_id``, ``text``, and metadata fields, or None if
        required fields are missing.
    """
    home = row.get("home_team_name") or row.get("home_team") or ""
    away = row.get("away_team_name") or row.get("away_team") or ""
    home_score = row.get("home_score") if row.get("home_score") is not None else row.get("home_team_score")
    away_score = row.get("away_score") if row.get("away_score") is not None else row.get("away_team_score")
    date = str(row.get("match_date", ""))[:10]
    venue = row.get("venue") or row.get("stadium") or "unknown venue"
    gameweek = row.get("gameweek") or row.get("round") or ""
    match_id = row.get("id") or row.get("match_id") or ""

    if not home or not away:
        return None

    gw_str = f"Gameweek {int(gameweek)}: " if gameweek and str(gameweek).isdigit() else ""
    score_str = (
        f"{home} {int(home_score)}-{int(away_score)} {away}"
        if pd.notna(home_score) and pd.notna(away_score)
        else f"{home} vs {away} (result pending)"
    )

    # Determine result narrative
    result_text = ""
    if pd.notna(home_score) and pd.notna(away_score):
        hs, as_ = int(home_score), int(away_score)
        if hs > as_:
            result_text = f"{home} won at home."
        elif as_ > hs:
            result_text = f"{away} won away from home."
        else:
            result_text = "The match ended in a draw."

    text = (
        f"Premier League 2025-26, {gw_str}{score_str} at {venue}. "
        f"{result_text}"
    ).strip()

    # Append any form/cumulative points if present
    for slug in ("villa", "liverpool"):
        form = row.get(f"{slug}_form")
        pts = row.get(f"{slug}_cumulative_points")
        if form and pd.notna(form):
            text += f" {slug.capitalize()} form: {form}."
        if pts and pd.notna(pts):
            text += f" {slug.capitalize()} cumulative points: {int(pts)}."

    chunk_id = f"match_stats_{match_id}" if match_id else f"match_stats_{date}_{home}_{away}".replace(" ", "_")

    return {
        "chunk_id": chunk_id,
        "text": text,
        "source_type": "match_stats",
        "source_id": str(match_id),
        "date": date,
        "modality": "text",
        "teams": ",".join(t for t in [home, away] if t),
        "players": "",
        "gameweek": int(gameweek) if str(gameweek).isdigit() else -1,
    }


def summarize_matches(matches_path: Path) -> list[dict[str, Any]]:
    """Load cleaned matches CSV and produce one summary per match.

    Args:
        matches_path: Path to ``data/cleaned/stats/matches.csv``.

    Returns:
        List of summary dicts, one per match row.
    """
    if not matches_path.exists():
        logger.warning("matches file not found", path=str(matches_path))
        return []

    df = pd.read_csv(matches_path)
    logger.info("loaded matches", rows=len(df))

    summaries = []
    for _, row in df.iterrows():
        summary = _summarize_match(row)
        if summary:
            summaries.append(summary)

    logger.info("generated match summaries", count=len(summaries))
    return summaries


# ---------------------------------------------------------------------------
# Player summary
# ---------------------------------------------------------------------------


def _summarize_player_row(row: pd.Series) -> dict[str, Any] | None:
    """Build a natural-language summary for a single player-gameweek row.

    Args:
        row: A row from a cleaned FPL player stats DataFrame.

    Returns:
        Summary dict or None if required fields are missing.
    """
    name = row.get("name") or row.get("player_name") or row.get("web_name") or ""
    team = row.get("team") or row.get("team_name") or ""
    gw = row.get("gw") or row.get("gameweek") or row.get("round") or ""
    goals = int(row.get("goals_scored", 0) or 0)
    assists = int(row.get("assists", 0) or 0)
    minutes = int(row.get("minutes", 0) or 0)
    points = row.get("total_points") or row.get("points")

    if not name:
        return None

    gw_str = f"Gameweek {int(gw)}" if str(gw).isdigit() else str(gw)
    pts_str = f" FPL points: {int(points)}." if points and pd.notna(points) else ""

    text = (
        f"{name} ({team}) — {gw_str}: "
        f"{goals} goal{'s' if goals != 1 else ''}, "
        f"{assists} assist{'s' if assists != 1 else ''}, "
        f"{minutes} minutes played.{pts_str}"
    )

    chunk_id = f"player_stats_{name}_{gw}".replace(" ", "_").lower()
    date = str(row.get("kickoff_time", ""))[:10] or ""

    return {
        "chunk_id": chunk_id,
        "text": text,
        "source_type": "player_stats",
        "source_id": chunk_id,
        "date": date,
        "modality": "text",
        "teams": team,
        "players": name,
        "gameweek": int(gw) if str(gw).isdigit() else -1,
    }


def summarize_players(players_dir: Path) -> list[dict[str, Any]]:
    """Load all cleaned FPL player CSVs and produce one summary per row.

    Args:
        players_dir: ``data/cleaned/stats/`` directory containing
            ``players_villa.csv`` and ``players_liverpool.csv``.

    Returns:
        Flat list of player summary dicts for both teams.
    """
    summaries: list[dict[str, Any]] = []

    for slug in ("villa", "liverpool"):
        path = players_dir / f"players_{slug}.csv"
        if not path.exists():
            logger.warning("player file not found", path=str(path))
            continue
        df = pd.read_csv(path)
        logger.info("loaded player stats", team=slug, rows=len(df))
        for _, row in df.iterrows():
            summary = _summarize_player_row(row)
            if summary:
                summaries.append(summary)

    logger.info("generated player summaries", count=len(summaries))
    return summaries


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_stats_summarization() -> dict[str, list[dict[str, Any]]]:
    """Generate and save match and player text summaries.

    Reads from ``data/cleaned/stats/``, writes two JSON files back to the
    same directory.

    Returns:
        Dict with ``matches`` and ``players`` summary lists.
    """
    cleaned_stats = settings.cleaned_dir / "stats"
    cleaned_stats.mkdir(parents=True, exist_ok=True)

    match_summaries = summarize_matches(cleaned_stats / "matches.csv")
    player_summaries = summarize_players(cleaned_stats)

    if match_summaries:
        out = cleaned_stats / "match_summaries.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(match_summaries, f, indent=2, ensure_ascii=False)
        logger.info("saved match summaries", path=str(out), count=len(match_summaries))

    if player_summaries:
        out = cleaned_stats / "player_summaries.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(player_summaries, f, indent=2, ensure_ascii=False)
        logger.info("saved player summaries", path=str(out), count=len(player_summaries))

    logger.info("stats summarization complete")
    return {"matches": match_summaries, "players": player_summaries}
