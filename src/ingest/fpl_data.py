"""
FPL player and per-gameweek stats ingestion.

Fetches player metadata and per-gameweek performance history from the official
Fantasy Premier League API (no API key required), filtered to Aston Villa and
Liverpool players only.

Endpoints used:
    GET https://fantasy.premierleague.com/api/bootstrap-static/
        → all player metadata and season totals
    GET https://fantasy.premierleague.com/api/element-summary/{id}/
        → per-gameweek history for a single player

Output:
    data/raw/stats/fpl/players.csv
    data/raw/stats/fpl/GW{n}/playerstats_gw.csv   (one file per completed GW)
"""
from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

import pandas as pd
import requests

from config.settings import settings
from config.teams import ASTON_VILLA, LIVERPOOL, FOCUS_TEAMS
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)

FPL_BASE = "https://fantasy.premierleague.com/api"

FOCUS_FPL_IDS = {t.fpl_id: t.name for t in FOCUS_TEAMS}

# Seconds to wait between element-summary requests to avoid rate limiting
_REQUEST_DELAY = 0.3


class FPLPlayerLoader:
    """Downloads and saves FPL player metadata and per-GW stats.

    Attributes:
        fpl_dir: Local directory for saving CSV output.
        session: Persistent HTTP session.
    """

    def __init__(self) -> None:
        self.fpl_dir = settings.raw_dir / "stats" / "fpl"
        self.fpl_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    @retry(max_attempts=5, base_delay=1.0)
    def _get(self, endpoint: str) -> Any:
        """GET a FPL API endpoint and return parsed JSON.

        Args:
            endpoint: Path relative to FPL_BASE.

        Returns:
            Parsed JSON response.
        """
        url = f"{FPL_BASE}{endpoint}"
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_players(self) -> pd.DataFrame:
        """Download focus-team player metadata from bootstrap-static.

        Filters to Aston Villa and Liverpool players, adds the team name as a
        column, and saves to fpl/players.csv.

        Returns:
            DataFrame of focus-team players with season-total stats.
        """
        logger.info("fetching bootstrap-static for players")
        bootstrap = self._get("/bootstrap-static/")
        elements = bootstrap["elements"]

        focus_players = [
            {**p, "team_name": FOCUS_FPL_IDS[p["team"]]}
            for p in elements
            if p["team"] in FOCUS_FPL_IDS
        ]

        df = pd.DataFrame(focus_players)
        df.to_csv(self.fpl_dir / "players.csv", index=False)
        logger.info("saved focus-team players", rows=len(df))
        return df

    @retry(max_attempts=3, base_delay=1.0)
    def _fetch_player_history(self, player_id: int) -> list[dict[str, Any]]:
        """Fetch the per-gameweek history for a single player.

        Args:
            player_id: FPL element ID.

        Returns:
            List of per-GW history records for this player.
        """
        data = self._get(f"/element-summary/{player_id}/")
        return data.get("history", [])

    def fetch_all_gw_stats(self, players_df: pd.DataFrame | None = None) -> int:
        """Fetch per-GW stats for all focus-team players and save by gameweek.

        For each completed gameweek, combines all focus-team player records
        into a single CSV at fpl/GW{n}/playerstats_gw.csv.

        Args:
            players_df: Pre-fetched focus-team players DataFrame. If None,
                calls fetch_players() first.

        Returns:
            Number of completed gameweeks saved.
        """
        if players_df is None:
            players_df = self.fetch_players()

        # Build ID → metadata map for enriching history records
        id_to_meta: dict[int, dict[str, Any]] = {}
        for _, row in players_df.iterrows():
            id_to_meta[int(row["id"])] = {
                "name": f"{row['first_name']} {row['second_name']}".strip(),
                "web_name": row.get("web_name", ""),
                "team": row.get("team_name", ""),
                "position": row.get("element_type", ""),
            }

        # Collect all per-GW records grouped by round
        gw_records: dict[int, list[dict[str, Any]]] = defaultdict(list)

        total_players = len(id_to_meta)
        for idx, (player_id, meta) in enumerate(id_to_meta.items(), 1):
            logger.info(
                "fetching player gw history",
                player=meta["name"],
                player_id=player_id,
                progress=f"{idx}/{total_players}",
            )
            history = self._fetch_player_history(player_id)
            for entry in history:
                record = {
                    "player_id": player_id,
                    "name": meta["name"],
                    "web_name": meta["web_name"],
                    "team": meta["team"],
                    "position": meta["position"],
                    "gw": entry.get("round"),
                    "fixture_id": entry.get("fixture"),
                    "kickoff_time": entry.get("kickoff_time"),
                    "opponent_team_id": entry.get("opponent_team"),
                    "was_home": entry.get("was_home"),
                    "minutes": entry.get("minutes", 0),
                    "goals_scored": entry.get("goals_scored", 0),
                    "assists": entry.get("assists", 0),
                    "clean_sheets": entry.get("clean_sheets", 0),
                    "goals_conceded": entry.get("goals_conceded", 0),
                    "own_goals": entry.get("own_goals", 0),
                    "penalties_saved": entry.get("penalties_saved", 0),
                    "penalties_missed": entry.get("penalties_missed", 0),
                    "yellow_cards": entry.get("yellow_cards", 0),
                    "red_cards": entry.get("red_cards", 0),
                    "saves": entry.get("saves", 0),
                    "bonus": entry.get("bonus", 0),
                    "bps": entry.get("bps", 0),
                    "total_points": entry.get("total_points"),
                    "expected_goals": entry.get("expected_goals"),
                    "expected_assists": entry.get("expected_assists"),
                    "expected_goal_involvements": entry.get("expected_goal_involvements"),
                    "influence": entry.get("influence"),
                    "creativity": entry.get("creativity"),
                    "threat": entry.get("threat"),
                    "ict_index": entry.get("ict_index"),
                    "starts": entry.get("starts", 0),
                    "value": entry.get("value"),
                    "selected": entry.get("selected"),
                }
                gw = entry.get("round")
                if gw:
                    gw_records[gw].append(record)

            time.sleep(_REQUEST_DELAY)

        # Save one CSV per completed gameweek
        for gw, records in sorted(gw_records.items()):
            gw_dir = self.fpl_dir / f"GW{gw}"
            gw_dir.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame(records)
            df.to_csv(gw_dir / "playerstats_gw.csv", index=False)
            logger.info("saved gw stats", gameweek=gw, rows=len(df))

        logger.info("all gw stats saved", gameweeks=len(gw_records))
        return len(gw_records)


def run_fpl_ingestion() -> None:
    """Orchestrate focus-team player metadata and per-GW stats download.

    Saves filtered CSVs to data/raw/stats/fpl/.
    """
    loader = FPLPlayerLoader()
    players_df = loader.fetch_players()
    loader.fetch_all_gw_stats(players_df)
    logger.info("fpl ingestion complete")
