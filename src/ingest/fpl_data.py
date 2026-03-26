"""
FPL-Core-Insights CSV loader.

Downloads per-gameweek player performance CSVs from the FPL-Core-Insights
GitHub repository and filters to only Aston Villa and Liverpool players.

Repository: https://github.com/olbauday/FPL-Core-Insights
Target path: data/2025-2026/
"""
from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config.settings import settings
from config.teams import FOCUS_TEAM_NAMES
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)

REPO_BASE = "https://raw.githubusercontent.com/olbauday/FPL-Core-Insights/main"
SEASON_PATH = "data/2025-2026"

# FPL uses slightly different team name spellings — map to canonical names
FPL_TEAM_NAME_MAP: dict[str, str] = {
    "Aston Villa": "Aston Villa",
    "Liverpool": "Liverpool",
    # Add more mappings if FPL uses abbreviated names
}


class FPLDataLoader:
    """Downloads and saves FPL CSV data for focus teams.

    Attributes:
        fpl_dir: Local directory to store downloaded FPL CSVs.
        session: Persistent HTTP session for GitHub raw content.
    """

    def __init__(self) -> None:
        """Initialize the loader, creating the output directory if needed."""
        self.fpl_dir = settings.raw_dir / "stats" / "fpl"
        self.fpl_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()

    @retry(max_attempts=5, base_delay=1.0)
    def _download_csv(self, relative_path: str) -> pd.DataFrame | None:
        """Download a CSV file from the FPL-Core-Insights GitHub repo.

        Args:
            relative_path: Path relative to the repo root (e.g. ``data/2025-2026/players.csv``).

        Returns:
            DataFrame with the CSV contents, or None if the file is not found.
        """
        url = f"{REPO_BASE}/{relative_path}"
        logger.info("downloading fpl csv", url=url)
        response = self.session.get(url, timeout=30)
        if response.status_code == 404:
            logger.warning("fpl csv not found", url=url)
            return None
        response.raise_for_status()
        return pd.read_csv(io.StringIO(response.text))

    def _filter_to_focus_teams(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter a DataFrame to rows belonging to focus teams.

        Tries common column names used in FPL datasets (``team``,
        ``team_name``, ``club``, ``Team``).

        Args:
            df: Raw FPL DataFrame potentially containing all teams.

        Returns:
            Filtered DataFrame with only Aston Villa and Liverpool rows.
        """
        team_columns = ["team", "team_name", "club", "Team"]
        team_col = next((c for c in team_columns if c in df.columns), None)
        if team_col is None:
            logger.warning("no team column found in fpl dataframe", columns=list(df.columns))
            return df

        # Build a case-insensitive match set for focus team names
        focus_lower = {n.lower() for n in FOCUS_TEAM_NAMES}
        mask = df[team_col].str.lower().isin(focus_lower)
        filtered = df[mask].copy()
        logger.info(
            "filtered fpl dataframe",
            original_rows=len(df),
            filtered_rows=len(filtered),
            team_col=team_col,
        )
        return filtered

    def fetch_players(self) -> pd.DataFrame | None:
        """Download player metadata CSV and save focus-team players.

        Returns:
            Filtered DataFrame of focus-team players, or None on failure.
        """
        df = self._download_csv(f"{SEASON_PATH}/players.csv")
        if df is None:
            return None
        filtered = self._filter_to_focus_teams(df)
        filtered.to_csv(self.fpl_dir / "players.csv", index=False)
        logger.info("saved fpl players", rows=len(filtered))
        return filtered

    def fetch_gameweeks(self) -> pd.DataFrame | None:
        """Download gameweek metadata CSV.

        Returns:
            DataFrame of gameweek dates/metadata, or None on failure.
        """
        df = self._download_csv(f"{SEASON_PATH}/gameweeks.csv")
        if df is None:
            return None
        df.to_csv(self.fpl_dir / "gameweeks.csv", index=False)
        logger.info("saved fpl gameweeks", rows=len(df))
        return df

    def fetch_gameweek_stats(self, gameweek: int) -> pd.DataFrame | None:
        """Download per-gameweek player stats CSV and filter to focus teams.

        Args:
            gameweek: Gameweek number (1-38).

        Returns:
            Filtered DataFrame for the requested gameweek, or None on failure.
        """
        path = f"{SEASON_PATH}/GW{gameweek}/playerstats_gw.csv"
        df = self._download_csv(path)
        if df is None:
            return None
        filtered = self._filter_to_focus_teams(df)
        gw_dir = self.fpl_dir / f"GW{gameweek}"
        gw_dir.mkdir(parents=True, exist_ok=True)
        filtered.to_csv(gw_dir / "playerstats_gw.csv", index=False)
        logger.info("saved gw stats", gameweek=gameweek, rows=len(filtered))
        return filtered

    def fetch_all_gameweek_stats(self, max_gameweeks: int = 38) -> list[pd.DataFrame]:
        """Download stats for all completed gameweeks.

        Iterates from GW1 to ``max_gameweeks``, stopping early if a gameweek
        CSV is not yet available (404).

        Args:
            max_gameweeks: Maximum gameweek number to attempt (default 38).

        Returns:
            List of filtered DataFrames for each completed gameweek.
        """
        results = []
        for gw in range(1, max_gameweeks + 1):
            df = self.fetch_gameweek_stats(gw)
            if df is None:
                logger.info("no more gameweek data, stopping", last_gw=gw - 1)
                break
            results.append(df)
        return results


def run_fpl_ingestion() -> None:
    """Orchestrate the full FPL data download.

    Downloads player metadata, gameweek metadata, and per-gameweek player
    stats, saving filtered CSVs to ``data/raw/stats/fpl/``.
    """
    loader = FPLDataLoader()
    loader.fetch_players()
    loader.fetch_gameweeks()
    loader.fetch_all_gameweek_stats()
    logger.info("fpl ingestion complete")
