"""
Neo4j knowledge graph store.

Provides Neo4jStore — a context-managed driver wrapper with write methods
for each node type in the knowledge graph. All writes use MERGE (idempotent)
and parameterised queries only.
"""
from __future__ import annotations

from typing import Any

import pandas as pd
from neo4j import GraphDatabase, Driver, Session

from config.settings import settings
from config.teams import FOCUS_TEAM_NAMES
from src.utils.logger import get_logger

logger = get_logger(__name__)

SEASON_LABEL = "2025-26"


class Neo4jStore:
    """Context-managed Neo4j driver with graph population methods.

    Usage::

        with Neo4jStore(uri, user, password) as store:
            store.create_season()
            store.create_teams(teams)

    Attributes:
        driver: The Neo4j ``Driver`` instance.
    """

    def __init__(self, uri: str, user: str, password: str) -> None:
        """Open a Neo4j driver connection.

        Args:
            uri: Bolt or Neo4j URI, e.g. ``bolt://localhost:7687``.
            user: Neo4j username.
            password: Neo4j password.
        """
        self.driver: Driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info("neo4j driver opened", uri=uri)

    def close(self) -> None:
        """Close the Neo4j driver."""
        self.driver.close()
        logger.info("neo4j driver closed")

    def __enter__(self) -> "Neo4jStore":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Season
    # ------------------------------------------------------------------

    def create_season(self) -> None:
        """MERGE the 2025-26 Season node."""

        def _tx(session: Session) -> None:
            session.execute_write(
                lambda tx: tx.run(
                    "MERGE (s:Season {label: $label})",
                    label=SEASON_LABEL,
                )
            )

        with self.driver.session() as session:
            _tx(session)
        logger.info("season node merged", season=SEASON_LABEL)

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    def create_teams(self, teams_data: list[dict[str, Any]]) -> None:
        """MERGE Team nodes — only focus teams are written.

        Args:
            teams_data: List of team dicts with at minimum ``name``,
                ``abbreviation``, ``stadium``, ``stadium_city``.
        """

        def _merge_team(tx: Any, team: dict[str, Any]) -> None:
            tx.run(
                """
                MERGE (t:Team {name: $name})
                SET t.abbreviation = $abbreviation,
                    t.stadium = $stadium,
                    t.city = $city
                """,
                name=team["name"],
                abbreviation=team.get("abbreviation", ""),
                stadium=team.get("stadium", ""),
                city=team.get("stadium_city", ""),
            )

        focus = [t for t in teams_data if t.get("name") in FOCUS_TEAM_NAMES]
        with self.driver.session() as session:
            for team in focus:
                session.execute_write(_merge_team, team)
        logger.info("team nodes merged", count=len(focus))

    # ------------------------------------------------------------------
    # Stadiums
    # ------------------------------------------------------------------

    def create_stadiums(self) -> None:
        """MERGE Stadium nodes for Villa Park and Anfield."""

        stadiums = [
            {"name": "Villa Park", "city": "Birmingham", "team": "Aston Villa"},
            {"name": "Anfield", "city": "Liverpool", "team": "Liverpool"},
        ]

        def _merge_stadium(tx: Any, s: dict[str, Any]) -> None:
            tx.run(
                """
                MERGE (st:Stadium {name: $name})
                SET st.city = $city
                WITH st
                MATCH (t:Team {name: $team})
                MERGE (t)-[:PLAYS_AT]->(st)
                """,
                name=s["name"],
                city=s["city"],
                team=s["team"],
            )

        with self.driver.session() as session:
            for s in stadiums:
                session.execute_write(_merge_stadium, s)
        logger.info("stadium nodes merged", count=len(stadiums))

    # ------------------------------------------------------------------
    # Players
    # ------------------------------------------------------------------

    def create_players(
        self, roster_data: list[dict[str, Any]], team_name: str
    ) -> None:
        """MERGE Player nodes for a team roster and link them to their Team.

        Args:
            roster_data: List of player dicts with at minimum ``id``,
                ``first_name``, ``last_name``, ``position``.
            team_name: Team name string used to look up the Team node.
        """

        def _merge_player(tx: Any, player: dict[str, Any], team: str) -> None:
            tx.run(
                """
                MERGE (p:Player {player_id: $player_id})
                SET p.first_name = $first_name,
                    p.last_name = $last_name,
                    p.position = $position
                WITH p
                MATCH (t:Team {name: $team})
                MERGE (p)-[:PLAYS_FOR]->(t)
                """,
                player_id=str(player.get("id", "")),
                first_name=player.get("first_name", ""),
                last_name=player.get("last_name", player.get("second_name", "")),
                position=player.get("position", player.get("element_type", "")),
                team=team_name,
            )

        with self.driver.session() as session:
            for player in roster_data:
                session.execute_write(_merge_player, player, team_name)
        logger.info("player nodes merged", team=team_name, count=len(roster_data))

    # ------------------------------------------------------------------
    # Gameweeks
    # ------------------------------------------------------------------

    def create_gameweeks(self, matches_df: pd.DataFrame) -> None:
        """MERGE Gameweek nodes from unique GW numbers in the matches DataFrame.

        Args:
            matches_df: Cleaned matches DataFrame (must contain a ``gameweek``
                column).
        """
        if "gameweek" not in matches_df.columns:
            logger.warning("no 'gameweek' column found in matches_df")
            return

        unique_gws = sorted(matches_df["gameweek"].dropna().unique().astype(int))

        def _merge_gw(tx: Any, gw: int) -> None:
            tx.run(
                """
                MERGE (g:Gameweek {number: $number})
                WITH g
                MATCH (s:Season {label: $season})
                MERGE (g)-[:PART_OF]->(s)
                """,
                number=gw,
                season=SEASON_LABEL,
            )

        with self.driver.session() as session:
            for gw in unique_gws:
                session.execute_write(_merge_gw, gw)
        logger.info("gameweek nodes merged", count=len(unique_gws))

    # ------------------------------------------------------------------
    # Matches
    # ------------------------------------------------------------------

    def create_matches(self, matches_df: pd.DataFrame) -> None:
        """MERGE Match nodes with HOME_TEAM, AWAY_TEAM, and PART_OF relationships.

        Args:
            matches_df: Cleaned matches DataFrame. Expected columns:
                ``id``, ``date``, ``home_team``, ``away_team``,
                ``home_score``, ``away_score``, ``gameweek``.
        """

        def _merge_match(tx: Any, row: dict[str, Any]) -> None:
            tx.run(
                """
                MERGE (m:Match {match_id: $match_id})
                SET m.date = $date,
                    m.home_score = $home_score,
                    m.away_score = $away_score
                WITH m
                MATCH (home:Team {name: $home_team})
                MERGE (home)-[:HOME_TEAM]->(m)
                WITH m
                MATCH (away:Team {name: $away_team})
                MERGE (away)-[:AWAY_TEAM]->(m)
                WITH m
                MATCH (g:Gameweek {number: $gameweek})
                MERGE (m)-[:PART_OF]->(g)
                """,
                match_id=str(row.get("id", "")),
                date=str(row.get("date", "")),
                home_score=int(row.get("home_score", row.get("home_team_score", 0)) or 0),
                away_score=int(row.get("away_score", row.get("visitor_team_score", 0)) or 0),
                home_team=str(row.get("home_team", "")),
                away_team=str(row.get("away_team", "")),
                gameweek=int(row.get("gameweek", 0) or 0),
            )

        with self.driver.session() as session:
            for _, row in matches_df.iterrows():
                session.execute_write(_merge_match, row.to_dict())
        logger.info("match nodes merged", count=len(matches_df))

    # ------------------------------------------------------------------
    # Podcast Episodes
    # ------------------------------------------------------------------

    def create_podcast_episode(self, episode: dict[str, Any]) -> None:
        """MERGE a PodcastEpisode node.

        Args:
            episode: Episode metadata dict with ``youtube_id``, ``title``,
                ``channel``, ``published_at``, ``duration_seconds``.
        """

        def _merge_ep(tx: Any) -> None:
            tx.run(
                """
                MERGE (e:PodcastEpisode {youtube_id: $youtube_id})
                SET e.title = $title,
                    e.channel = $channel,
                    e.published_at = $published_at,
                    e.duration_seconds = $duration_seconds
                """,
                youtube_id=episode["youtube_id"],
                title=episode.get("title", ""),
                channel=episode.get("channel", ""),
                published_at=episode.get("published_at", ""),
                duration_seconds=int(episode.get("duration_seconds", 0)),
            )

        with self.driver.session() as session:
            session.execute_write(lambda tx: _merge_ep(tx))
        logger.info(
            "podcast episode merged",
            youtube_id=episode.get("youtube_id"),
            title=episode.get("title", "")[:50],
        )
