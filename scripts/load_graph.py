"""
Neo4j graph loader.

Reads cleaned match and roster data and populates the knowledge graph
by calling Neo4jStore methods in dependency order:
  Season → Teams → Stadiums → Players → Gameweeks → Matches

After loading, queries and logs node counts for all types.

Usage:
    python scripts/load_graph.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings
from config.teams import FOCUS_TEAMS
from src.store.neo4j_store import Neo4jStore
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

MATCHES_CSV = settings.cleaned_dir / "stats" / "matches.csv"
FPL_PLAYERS_CSV = settings.raw_dir / "stats" / "fpl" / "players.csv"
FPL_GW_DIR = settings.raw_dir / "stats" / "fpl"
PODCAST_MANIFEST = settings.raw_dir / "transcripts" / "podcast_episodes.json"

POSITION_MAP = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}


def _load_rosters() -> dict[str, list[dict]]:
    """Load focus-team players from the FPL players CSV.

    Returns:
        Dict mapping team_name → list of player dicts.
    """
    if not FPL_PLAYERS_CSV.exists():
        logger.warning("fpl players CSV not found", path=str(FPL_PLAYERS_CSV))
        return {}

    df = pd.read_csv(FPL_PLAYERS_CSV)
    rosters: dict[str, list[dict]] = {}

    STAT_COLS = [
        "goals_scored", "assists", "clean_sheets", "minutes", "yellow_cards",
        "red_cards", "expected_goals", "expected_assists", "total_points",
        "form", "points_per_game", "now_cost", "starts", "influence",
        "creativity", "threat", "ict_index",
    ]

    for team_name, group in df.groupby("team_name"):
        players = []
        for _, row in group.iterrows():
            pos_code = int(row.get("element_type", 0))
            player = {
                "id": str(row["id"]),
                "first_name": row.get("first_name", ""),
                "last_name": row.get("second_name", ""),
                "web_name": row.get("web_name", ""),
                "position": POSITION_MAP.get(pos_code, str(pos_code)),
            }
            for col in STAT_COLS:
                player[col] = row.get(col, 0)
            players.append(player)
        rosters[team_name] = players
        logger.info("loaded roster", team=team_name, players=len(players))
    return rosters


def _load_gw_stats() -> pd.DataFrame:
    """Load all per-GW player stat CSVs into a single DataFrame.

    Returns:
        Combined DataFrame of all GW appearance records, or empty DataFrame
        if no files are found.
    """
    gw_files = sorted(FPL_GW_DIR.glob("GW*/playerstats_gw.csv"))
    if not gw_files:
        logger.warning("no GW stat files found", path=str(FPL_GW_DIR))
        return pd.DataFrame()

    frames = []
    for f in gw_files:
        try:
            frames.append(pd.read_csv(f))
        except Exception as exc:
            logger.warning("skipping GW file", path=str(f), error=str(exc))

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    # Drop duplicate appearances (same player + fixture from multiple GW files)
    combined = combined.drop_duplicates(subset=["player_id", "fixture_id"])
    logger.info("loaded gw stats", files=len(frames), rows=len(combined))
    return combined


def _log_node_counts(store: Neo4jStore) -> None:
    """Query and log node counts for every label in the graph.

    Args:
        store: Open Neo4jStore instance.
    """
    with store.driver.session() as session:
        result = session.run(
            "MATCH (n) RETURN labels(n)[0] AS type, count(n) AS count "
            "ORDER BY count DESC"
        )
        rows = result.data()

    if not rows:
        logger.info("graph is empty — no nodes found")
        return

    logger.info("graph node counts")
    for row in rows:
        logger.info("  node type", type=row["type"], count=row["count"])


def main() -> None:
    """Load all cleaned data into Neo4j and report node counts."""
    if not MATCHES_CSV.exists():
        logger.error("matches CSV not found — run Phase 2 first", path=str(MATCHES_CSV))
        sys.exit(1)

    matches_df = pd.read_csv(MATCHES_CSV)
    logger.info("loaded matches", rows=len(matches_df))

    rosters = _load_rosters()
    gw_stats = _load_gw_stats()

    teams_data = [
        {
            "name": t.name,
            "abbreviation": t.abbreviation,
            "stadium": t.stadium,
            "stadium_city": t.stadium_city,
        }
        for t in FOCUS_TEAMS
    ]

    store = Neo4jStore(
        settings.neo4j_uri,
        settings.neo4j_user,
        settings.neo4j_password,
    )
    try:
        logger.info("loading: season")
        store.create_season()

        logger.info("loading: teams")
        store.create_teams(teams_data)

        logger.info("loading: stadiums")
        store.create_stadiums()

        logger.info("loading: players")
        for team_name, players in rosters.items():
            store.create_players(players, team_name)

        logger.info("loading: gameweeks")
        store.create_gameweeks(matches_df)

        logger.info("loading: matches")
        store.create_matches(matches_df)

        if not gw_stats.empty:
            # Only keep appearances for fixtures that exist as Match nodes
            valid_ids = set(matches_df["id"].astype(str))
            filtered = gw_stats[gw_stats["fixture_id"].astype(str).isin(valid_ids)]
            dropped = len(gw_stats) - len(filtered)
            if dropped:
                logger.info("dropped appearances for non-PL fixtures", count=dropped)
            logger.info("loading: player appearances", rows=len(filtered))
            store.create_player_appearances(filtered)

        # Load podcast episodes if manifest exists
        if PODCAST_MANIFEST.exists():
            with open(PODCAST_MANIFEST, encoding="utf-8") as f:
                episodes = json.load(f)
            logger.info("loading: podcast episodes", count=len(episodes))
            for ep in episodes:
                store.create_podcast_episode_with_season(ep)
            logger.info("podcast episodes loaded", count=len(episodes))
        else:
            logger.info("no podcast manifest found, skipping")

        logger.info("all data loaded — querying node counts")
        _log_node_counts(store)

    finally:
        store.close()

    logger.info("load_graph complete")


if __name__ == "__main__":
    main()
