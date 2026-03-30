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
                WITH t
                MATCH (s:Season {label: $season})
                MERGE (t)-[:IN_SEASON]->(s)
                """,
                name=team["name"],
                abbreviation=team.get("abbreviation", ""),
                stadium=team.get("stadium", ""),
                city=team.get("stadium_city", ""),
                season=SEASON_LABEL,
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
                    p.web_name = $web_name,
                    p.position = $position,
                    p.goals_scored = $goals_scored,
                    p.assists = $assists,
                    p.clean_sheets = $clean_sheets,
                    p.minutes = $minutes,
                    p.yellow_cards = $yellow_cards,
                    p.red_cards = $red_cards,
                    p.expected_goals = $expected_goals,
                    p.expected_assists = $expected_assists,
                    p.total_points = $total_points,
                    p.form = $form,
                    p.points_per_game = $points_per_game,
                    p.now_cost = $now_cost,
                    p.starts = $starts,
                    p.influence = $influence,
                    p.creativity = $creativity,
                    p.threat = $threat,
                    p.ict_index = $ict_index
                WITH p
                MATCH (t:Team {name: $team})
                MERGE (p)-[:PLAYS_FOR]->(t)
                """,
                player_id=str(player.get("id", "")),
                first_name=player.get("first_name", ""),
                last_name=player.get("last_name", player.get("second_name", "")),
                web_name=player.get("web_name", ""),
                position=player.get("position", player.get("element_type", "")),
                goals_scored=int(player.get("goals_scored", 0) or 0),
                assists=int(player.get("assists", 0) or 0),
                clean_sheets=int(player.get("clean_sheets", 0) or 0),
                minutes=int(player.get("minutes", 0) or 0),
                yellow_cards=int(player.get("yellow_cards", 0) or 0),
                red_cards=int(player.get("red_cards", 0) or 0),
                expected_goals=float(player.get("expected_goals", 0.0) or 0.0),
                expected_assists=float(player.get("expected_assists", 0.0) or 0.0),
                total_points=int(player.get("total_points", 0) or 0),
                form=float(player.get("form", 0.0) or 0.0),
                points_per_game=float(player.get("points_per_game", 0.0) or 0.0),
                now_cost=int(player.get("now_cost", 0) or 0),
                starts=int(player.get("starts", 0) or 0),
                influence=float(player.get("influence", 0.0) or 0.0),
                creativity=float(player.get("creativity", 0.0) or 0.0),
                threat=float(player.get("threat", 0.0) or 0.0),
                ict_index=float(player.get("ict_index", 0.0) or 0.0),
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
                    m.away_score = $away_score,
                    m.home_team_name = $home_team,
                    m.away_team_name = $away_team,
                    m.gameweek = $gameweek
                WITH m
                OPTIONAL MATCH (home:Team {name: $home_team})
                FOREACH (_ IN CASE WHEN home IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (home)-[:HOME_TEAM]->(m)
                )
                WITH m
                OPTIONAL MATCH (away:Team {name: $away_team})
                FOREACH (_ IN CASE WHEN away IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (away)-[:AWAY_TEAM]->(m)
                )
                WITH m
                MATCH (g:Gameweek {number: $gameweek})
                MERGE (m)-[:PART_OF]->(g)
                """,
                match_id=str(row.get("id", "")),
                date=str(row.get("match_date", row.get("date", ""))),
                home_score=int(hs) if (hs := row.get("home_score", row.get("home_team_score"))) is not None and str(hs).lower() != "nan" else None,
                away_score=int(as_) if (as_ := row.get("away_score", row.get("visitor_team_score"))) is not None and str(as_).lower() != "nan" else None,
                home_team=str(row.get("home_team_name", row.get("home_team", ""))),
                away_team=str(row.get("away_team_name", row.get("away_team", ""))),
                gameweek=int(row.get("gameweek", 0) or 0),
            )

        with self.driver.session() as session:
            for _, row in matches_df.iterrows():
                session.execute_write(_merge_match, row.to_dict())
        logger.info("match nodes merged", count=len(matches_df))

    # ------------------------------------------------------------------
    # Player Appearances (per-GW performance linking Player → Match)
    # ------------------------------------------------------------------

    def create_player_appearances(self, gw_df: pd.DataFrame) -> None:
        """MERGE PlayerAppearance nodes and link them to Players and Matches.

        Each row represents one player's stats in one fixture. Creates:
            (p:Player)-[:HAD_APPEARANCE]->(a:PlayerAppearance)-[:IN_MATCH]->(m:Match)

        Args:
            gw_df: Combined GW stats DataFrame with columns:
                ``player_id``, ``fixture_id``, ``gw``, ``was_home``,
                ``minutes``, ``goals_scored``, ``assists``, ``clean_sheets``,
                ``goals_conceded``, ``yellow_cards``, ``red_cards``, ``saves``,
                ``bonus``, ``bps``, ``total_points``, ``expected_goals``,
                ``expected_assists``, ``starts``.
        """

        def _merge_appearance(tx: Any, row: dict[str, Any]) -> None:
            tx.run(
                """
                MERGE (a:PlayerAppearance {appearance_id: $appearance_id})
                SET a.name = $name,
                    a.gw = $gw,
                    a.was_home = $was_home,
                    a.minutes = $minutes,
                    a.goals_scored = $goals_scored,
                    a.assists = $assists,
                    a.clean_sheets = $clean_sheets,
                    a.goals_conceded = $goals_conceded,
                    a.yellow_cards = $yellow_cards,
                    a.red_cards = $red_cards,
                    a.saves = $saves,
                    a.bonus = $bonus,
                    a.bps = $bps,
                    a.total_points = $total_points,
                    a.expected_goals = $expected_goals,
                    a.expected_assists = $expected_assists,
                    a.starts = $starts
                WITH a
                MATCH (p:Player {player_id: $player_id})
                MERGE (p)-[:HAD_APPEARANCE]->(a)
                WITH a
                MATCH (m:Match {match_id: $match_id})
                MERGE (a)-[:IN_MATCH]->(m)
                """,
                appearance_id=f"{row['player_id']}_{row['fixture_id']}",
                name=f"{row.get('web_name', row['player_id'])} GW{int(row.get('gw', 0) or 0)}",
                gw=int(row.get("gw", 0) or 0),
                was_home=bool(row.get("was_home", False)),
                minutes=int(row.get("minutes", 0) or 0),
                goals_scored=int(row.get("goals_scored", 0) or 0),
                assists=int(row.get("assists", 0) or 0),
                clean_sheets=int(row.get("clean_sheets", 0) or 0),
                goals_conceded=int(row.get("goals_conceded", 0) or 0),
                yellow_cards=int(row.get("yellow_cards", 0) or 0),
                red_cards=int(row.get("red_cards", 0) or 0),
                saves=int(row.get("saves", 0) or 0),
                bonus=int(row.get("bonus", 0) or 0),
                bps=int(row.get("bps", 0) or 0),
                total_points=int(row.get("total_points", 0) or 0),
                expected_goals=float(row.get("expected_goals", 0.0) or 0.0),
                expected_assists=float(row.get("expected_assists", 0.0) or 0.0),
                starts=int(row.get("starts", 0) or 0),
                player_id=str(row["player_id"]),
                match_id=str(row["fixture_id"]),
            )

        with self.driver.session() as session:
            for _, row in gw_df.iterrows():
                session.execute_write(_merge_appearance, row.to_dict())
        logger.info("player appearance nodes merged", count=len(gw_df))

    # ------------------------------------------------------------------
    # Podcast Episodes
    # ------------------------------------------------------------------

    def create_podcast_episode(self, episode: dict[str, Any]) -> None:
        """MERGE a PodcastEpisode node."""

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

    def create_podcast_episode_with_season(self, episode: dict[str, Any]) -> None:
        """MERGE a PodcastEpisode node with a COVERS_SEASON relationship.

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
                WITH e
                MATCH (s:Season {label: $season})
                MERGE (e)-[:COVERS_SEASON]->(s)
                """,
                youtube_id=episode["youtube_id"],
                title=episode.get("title", ""),
                channel=episode.get("channel", ""),
                published_at=episode.get("published_at", ""),
                duration_seconds=int(episode.get("duration_seconds", 0)),
                season=SEASON_LABEL,
            )

        with self.driver.session() as session:
            session.execute_write(lambda tx: _merge_ep(tx))
        logger.info(
            "podcast episode merged with season",
            youtube_id=episode.get("youtube_id"),
            title=episode.get("title", "")[:50],
        )

