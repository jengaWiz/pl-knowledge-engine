"""
Neo4j graph seed script.

Creates all constraints and indexes required by the knowledge graph schema.
Safe to run multiple times — all statements use IF NOT EXISTS.

Usage:
    python scripts/seed_graph.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from neo4j import GraphDatabase

from config.settings import settings
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Schema DDL — 8 constraints + 3 indexes
# ---------------------------------------------------------------------------

CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Season) REQUIRE s.label IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Team) REQUIRE t.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (st:Stadium) REQUIRE st.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Player) REQUIRE p.player_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Gameweek) REQUIRE g.number IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Match) REQUIRE m.match_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (e:PodcastEpisode) REQUIRE e.youtube_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (mgr:Manager) REQUIRE mgr.name IS UNIQUE",
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS FOR (m:Match) ON (m.date)",
    "CREATE INDEX IF NOT EXISTS FOR (p:Player) ON (p.last_name)",
    "CREATE INDEX IF NOT EXISTS FOR (e:PodcastEpisode) ON (e.published_at)",
]


def main() -> None:
    """Execute all constraint and index DDL statements idempotently."""
    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    try:
        with driver.session() as session:
            for stmt in CONSTRAINTS:
                session.run(stmt)
                logger.info("constraint applied", stmt=stmt[:60])

            for stmt in INDEXES:
                session.run(stmt)
                logger.info("index applied", stmt=stmt[:60])

        logger.info(
            "seed complete",
            constraints=len(CONSTRAINTS),
            indexes=len(INDEXES),
        )
    finally:
        driver.close()


if __name__ == "__main__":
    main()
