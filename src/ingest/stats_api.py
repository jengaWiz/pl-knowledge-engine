"""
FPL match and standings ingestion.

Fetches teams, standings, and match fixtures from the official Fantasy Premier
League API (no API key required) and saves raw JSON to data/raw/stats/.

Endpoints used:
    GET https://fantasy.premierleague.com/api/bootstrap-static/
        → teams metadata and season standings
    GET https://fantasy.premierleague.com/api/fixtures/
        → all 380 season fixtures with scores and stats

Output:
    data/raw/stats/teams.json
    data/raw/stats/standings.json
    data/raw/stats/matches_villa.json
    data/raw/stats/matches_liverpool.json
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from config.settings import settings
from config.teams import ASTON_VILLA, LIVERPOOL, FOCUS_TEAMS
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)

FPL_BASE = "https://fantasy.premierleague.com/api"

FOCUS_FPL_IDS = {t.fpl_id: t.name for t in FOCUS_TEAMS}


class FPLMatchClient:
    """Fetches team, standings, and fixture data from the FPL API.

    Attributes:
        raw_stats_dir: Local directory for saving raw JSON output.
        session: Persistent HTTP session.
    """

    def __init__(self) -> None:
        self.raw_stats_dir = settings.raw_dir / "stats"
        self.raw_stats_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
        })

    @retry(max_attempts=5, base_delay=1.0)
    def _get(self, endpoint: str) -> Any:
        """GET a FPL API endpoint and return parsed JSON.

        Args:
            endpoint: Path relative to FPL_BASE (e.g. ``/bootstrap-static/``).

        Returns:
            Parsed JSON response.
        """
        url = f"{FPL_BASE}{endpoint}"
        logger.info("fetching fpl endpoint", url=url)
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()

    def _save_json(self, data: Any, path: Path) -> None:
        """Serialize data to JSON and write to path."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info("saved json", path=str(path), records=len(data) if isinstance(data, list) else 1)

    def fetch_teams(self) -> list[dict[str, Any]]:
        """Fetch all 20 PL teams and save to teams.json.

        Returns:
            List of team records from the FPL API.
        """
        bootstrap = self._get("/bootstrap-static/")
        teams = bootstrap["teams"]
        self._save_json(teams, self.raw_stats_dir / "teams.json")
        logger.info("fetched teams", count=len(teams))
        return teams

    def fetch_standings(self, teams: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        """Extract standings from bootstrap teams data and save to standings.json.

        Args:
            teams: Pre-fetched teams list. If None, fetches bootstrap again.

        Returns:
            List of standing records (one per team).
        """
        if teams is None:
            bootstrap = self._get("/bootstrap-static/")
            teams = bootstrap["teams"]

        standings = [
            {
                "position": t["position"],
                "team_name": t["name"],
                "short_name": t["short_name"],
                "team_id": t["id"],
                "played": t["played"],
                "win": t["win"],
                "draw": t["draw"],
                "loss": t["loss"],
                "points": t["points"],
                "strength": t["strength"],
            }
            for t in sorted(teams, key=lambda x: x["position"])
        ]
        self._save_json(standings, self.raw_stats_dir / "standings.json")
        logger.info("saved standings", teams=len(standings))
        return standings

    def fetch_matches(
        self, teams: list[dict[str, Any]] | None = None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Fetch all fixtures and filter to Villa and Liverpool matches.

        Enriches raw fixture records (which use numeric team IDs) with full
        team names and a clean match_date field before saving.

        Args:
            teams: Pre-fetched teams list used to build ID→name map.
                   If None, fetches bootstrap again.

        Returns:
            Tuple of (villa_matches, liverpool_matches).
        """
        if teams is None:
            bootstrap = self._get("/bootstrap-static/")
            teams = bootstrap["teams"]

        id_to_name = {t["id"]: t["name"] for t in teams}
        id_to_short = {t["id"]: t["short_name"] for t in teams}

        fixtures = self._get("/fixtures/")
        logger.info("fetched fixtures", total=len(fixtures))

        villa_id = ASTON_VILLA.fpl_id
        liverpool_id = LIVERPOOL.fpl_id

        villa_matches: list[dict[str, Any]] = []
        liverpool_matches: list[dict[str, Any]] = []

        for f in fixtures:
            home_id = f["team_h"]
            away_id = f["team_a"]

            enriched = {
                "id": f["id"],
                "gameweek": f.get("event"),
                "match_date": (f.get("kickoff_time") or "")[:10],
                "kickoff_time": f.get("kickoff_time"),
                "home_team_name": id_to_name.get(home_id, str(home_id)),
                "away_team_name": id_to_name.get(away_id, str(away_id)),
                "home_team_short": id_to_short.get(home_id, ""),
                "away_team_short": id_to_short.get(away_id, ""),
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home_score": f.get("team_h_score"),
                "away_score": f.get("team_a_score"),
                "finished": f.get("finished", False),
                "minutes": f.get("minutes", 0),
            }

            if home_id == villa_id or away_id == villa_id:
                villa_matches.append(enriched)
            if home_id == liverpool_id or away_id == liverpool_id:
                liverpool_matches.append(enriched)

        self._save_json(villa_matches, self.raw_stats_dir / "matches_villa.json")
        self._save_json(liverpool_matches, self.raw_stats_dir / "matches_liverpool.json")
        logger.info(
            "filtered matches",
            villa=len(villa_matches),
            liverpool=len(liverpool_matches),
        )
        return villa_matches, liverpool_matches


def run_stats_ingestion() -> None:
    """Orchestrate teams, standings, and fixtures ingestion from the FPL API.

    Saves raw JSON to data/raw/stats/ for downstream cleaning.
    """
    client = FPLMatchClient()
    teams = client.fetch_teams()
    client.fetch_standings(teams)
    client.fetch_matches(teams)
    logger.info("stats ingestion complete")
