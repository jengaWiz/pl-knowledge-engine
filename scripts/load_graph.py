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
ROSTER_PATTERN = "data/raw/stats/roster_*.json"


def _load_rosters() -> dict[str, list[dict]]:
    """Load all roster JSON files from data/raw/stats/.

    Returns:
        Dict mapping team_name → list of player dicts.
    """
    rosters: dict[str, list[dict]] = {}
    for path in sorted(Path(".").glob(ROSTER_PATTERN)):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            # Expect list of dicts with a 'team_name' key, or infer from filename
            team_name = path.stem.replace("roster_", "").replace("_", " ").title()
            rosters[team_name] = data if isinstance(data, list) else data.get("data", [])
            logger.info("loaded roster", team=team_name, players=len(rosters[team_name]))
        except Exception as exc:
            logger.warning("failed to load roster", path=str(path), error=str(exc))
    return rosters


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

        logger.info("all data loaded — querying node counts")
        _log_node_counts(store)

    finally:
        store.close()

    logger.info("load_graph complete")


if __name__ == "__main__":
    main()
