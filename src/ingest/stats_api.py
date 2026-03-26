"""
BallDontLie EPL API client for Premier League stats.

Fetches teams, standings, matches, rosters, and lineups from:
    https://api.balldontlie.io/epl/v2

Authentication is performed via the ``Authorization`` header using the
API key from ``config.settings``. All paginated requests use cursor-based
pagination (``meta.next_cursor`` / ``?cursor=`` parameter).
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests

from config.settings import settings
from config.teams import FOCUS_TEAM_NAMES
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)

BASE_URL = "https://api.balldontlie.io/epl/v2"
SEASON = 2025  # 2025-26 season


class BallDontLieClient:
    """HTTP client for the BallDontLie EPL API.

    All public methods save their raw responses to ``data/raw/stats/``
    and return the parsed list of data records.

    Attributes:
        session: Persistent requests session with auth header configured.
        raw_stats_dir: Directory for saving raw JSON API responses.
        lineups_dir: Sub-directory for per-match lineup files.
    """

    def __init__(self) -> None:
        """Initialize the API client with credentials from settings."""
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": settings.balldontlie_api_key}
        )
        self.raw_stats_dir = settings.raw_dir / "stats"
        self.lineups_dir = self.raw_stats_dir / "lineups"
        self.raw_stats_dir.mkdir(parents=True, exist_ok=True)
        self.lineups_dir.mkdir(parents=True, exist_ok=True)

    @retry(max_attempts=5, base_delay=1.0)
    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Perform a GET request against the BallDontLie API.

        Args:
            endpoint: API endpoint path (relative to BASE_URL), e.g. ``/teams``.
            params: Optional query parameters to include in the request.

        Returns:
            Parsed JSON response dictionary.

        Raises:
            requests.HTTPError: If the server returns a non-2xx status code.
        """
        url = f"{BASE_URL}{endpoint}"
        response = self.session.get(url, params=params or {})
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning(
                "rate limited, sleeping",
                endpoint=endpoint,
                retry_after=retry_after,
            )
            time.sleep(retry_after)
            response = self.session.get(url, params=params or {})
        response.raise_for_status()
        return response.json()

    def _get_all_pages(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Fetch all pages of a paginated endpoint using cursor-based pagination.

        Args:
            endpoint: API endpoint path relative to BASE_URL.
            params: Base query parameters (cursor will be added automatically).

        Returns:
            Concatenated list of all ``data`` records across all pages.
        """
        params = dict(params or {})
        all_records: list[dict[str, Any]] = []
        page = 1

        while True:
            response = self._get(endpoint, params)
            records = response.get("data", [])
            all_records.extend(records)

            meta = response.get("meta", {})
            next_cursor = meta.get("next_cursor")
            logger.info(
                "fetched page",
                endpoint=endpoint,
                page=page,
                records=len(records),
                total_so_far=len(all_records),
                next_cursor=next_cursor,
            )

            if not next_cursor:
                break
            params["cursor"] = next_cursor
            page += 1

        return all_records

    def _save_json(self, data: Any, filepath: Path) -> None:
        """Serialize data to a JSON file.

        Args:
            data: Data to serialize (must be JSON-serializable).
            filepath: Absolute destination path for the JSON file.
        """
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info("saved json", path=str(filepath), records=len(data) if isinstance(data, list) else 1)

    def fetch_teams(self) -> list[dict[str, Any]]:
        """Fetch all EPL teams and save to ``data/raw/stats/teams.json``.

        Returns:
            List of team records from the API.
        """
        logger.info("fetching teams")
        teams = self._get_all_pages("/teams")
        self._save_json(teams, self.raw_stats_dir / "teams.json")
        return teams

    def fetch_standings(self) -> list[dict[str, Any]]:
        """Fetch current season standings and save to ``data/raw/stats/standings.json``.

        Returns:
            List of standing records for the 2025-26 season.
        """
        logger.info("fetching standings", season=SEASON)
        standings = self._get_all_pages("/standings", {"season": SEASON})
        self._save_json(standings, self.raw_stats_dir / "standings.json")
        return standings

    def fetch_matches(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Fetch all season matches and filter to focus teams.

        Fetches every match for the 2025-26 season, then filters to only
        those involving Aston Villa or Liverpool. Saves separate JSON files
        for each team.

        Returns:
            Tuple of (villa_matches, liverpool_matches).
        """
        logger.info("fetching all matches", season=SEASON)
        all_matches = self._get_all_pages("/matches", {"season": SEASON})

        villa_matches = [
            m for m in all_matches
            if self._match_involves_team(m, "Aston Villa")
        ]
        liverpool_matches = [
            m for m in all_matches
            if self._match_involves_team(m, "Liverpool")
        ]

        self._save_json(villa_matches, self.raw_stats_dir / "matches_villa.json")
        self._save_json(liverpool_matches, self.raw_stats_dir / "matches_liverpool.json")
        logger.info(
            "filtered matches",
            villa=len(villa_matches),
            liverpool=len(liverpool_matches),
        )
        return villa_matches, liverpool_matches

    def _match_involves_team(self, match: dict[str, Any], team_name: str) -> bool:
        """Check whether a match record involves the specified team.

        Args:
            match: Raw match record from the API.
            team_name: Full team name to search for (case-insensitive).

        Returns:
            True if the team is either the home or away side in this match.
        """
        home = (match.get("home_team") or {}).get("name", "")
        away = (match.get("away_team") or {}).get("name", "")
        team_lower = team_name.lower()
        return team_lower in home.lower() or team_lower in away.lower()

    @retry(max_attempts=5, base_delay=1.0)
    def fetch_match_lineup(self, match_id: int) -> dict[str, Any]:
        """Fetch the lineup for a single match and save to lineups directory.

        Args:
            match_id: BallDontLie match identifier.

        Returns:
            Raw lineup data dictionary for the match.
        """
        logger.info("fetching lineup", match_id=match_id)
        response = self._get("/match_lineups", {"match_id": match_id})
        lineup_data = response.get("data", response)
        self._save_json(lineup_data, self.lineups_dir / f"{match_id}.json")
        return lineup_data

    def fetch_all_lineups(
        self,
        villa_matches: list[dict[str, Any]],
        liverpool_matches: list[dict[str, Any]],
    ) -> None:
        """Fetch lineups for all focus-team matches.

        Combines the match lists, deduplicates by match ID (head-to-head
        matches appear in both lists), and downloads each lineup file.

        Args:
            villa_matches: List of Aston Villa match records.
            liverpool_matches: List of Liverpool match records.
        """
        all_matches = {m["id"]: m for m in villa_matches + liverpool_matches}
        logger.info("fetching lineups", total_matches=len(all_matches))
        for match_id in all_matches:
            self.fetch_match_lineup(match_id)
            time.sleep(0.5)  # Be polite to the API

    def fetch_roster(self, team_id: int, team_name: str) -> list[dict[str, Any]]:
        """Fetch all players for a team and save to ``data/raw/stats/``.

        Args:
            team_id: BallDontLie team identifier.
            team_name: Human-readable team name used in the output filename.

        Returns:
            List of player records for the team.
        """
        logger.info("fetching roster", team_id=team_id, team=team_name)
        players = self._get_all_pages("/players", {"team_id": team_id})
        slug = team_name.lower().replace(" ", "_")
        self._save_json(players, self.raw_stats_dir / f"roster_{slug}.json")
        return players

    def fetch_all_rosters(self, teams: list[dict[str, Any]]) -> None:
        """Fetch rosters for Aston Villa and Liverpool from the full team list.

        Identifies the focus teams by name, then calls ``fetch_roster`` for
        each one.

        Args:
            teams: Full list of team records previously returned by ``fetch_teams``.
        """
        focus_lower = {n.lower() for n in FOCUS_TEAM_NAMES}
        for team in teams:
            name = team.get("name", "")
            if name.lower() in focus_lower:
                self.fetch_roster(team["id"], name)


def run_stats_ingestion() -> None:
    """Orchestrate the full stats ingestion pipeline.

    Fetches teams, standings, matches, lineups, and rosters in sequence,
    saving all raw API responses to ``data/raw/stats/``.
    """
    client = BallDontLieClient()

    teams = client.fetch_teams()
    client.fetch_standings()
    villa_matches, liverpool_matches = client.fetch_matches()
    client.fetch_all_lineups(villa_matches, liverpool_matches)
    client.fetch_all_rosters(teams)

    logger.info("stats ingestion complete")
