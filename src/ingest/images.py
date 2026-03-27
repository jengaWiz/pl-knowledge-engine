"""
Image downloader.

Downloads player headshots and team badges using public FPL (Fantasy Premier
League) API endpoints. No authentication required.

FPL bootstrap: GET https://fantasy.premierleague.com/api/bootstrap-static/
Player photos: https://resources.premierleague.com/premierleague/photos/players/110x140/p{photo_code}.png
Team badges:   https://resources.premierleague.com/premierleague/badges/rb/{team_code}.png

Output:
    data/raw/images/players/{player_name}.png
    data/raw/images/badges/{team_name}.png
    data/raw/images/images_metadata.json
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import requests

from config.settings import settings
from config.teams import FOCUS_TEAM_NAMES
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)

FPL_BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
PLAYER_PHOTO_URL = "https://resources.premierleague.com/premierleague/photos/players/110x140/p{code}.png"
BADGE_URL = "https://resources.premierleague.com/premierleague/badges/rb/{code}.png"


def _safe_filename(name: str) -> str:
    """Convert a player or team name to a safe filesystem filename.

    Args:
        name: Raw name string (may contain spaces, accents).

    Returns:
        Lowercase, underscore-separated ASCII filename string.
    """
    return re.sub(r"[^a-z0-9_]", "", name.lower().replace(" ", "_"))


class ImageDownloader:
    """Downloads player and badge images from the FPL CDN.

    Attributes:
        players_dir: Directory for player headshot PNG files.
        badges_dir: Directory for team badge PNG files.
        session: Persistent HTTP session.
        checkpoint: Tracks which items have been downloaded.
        metadata: Accumulated image metadata records.
    """

    def __init__(self) -> None:
        """Initialise directories and HTTP session."""
        images_dir = settings.raw_dir / "images"
        self.players_dir = images_dir / "players"
        self.badges_dir = images_dir / "badges"
        self.players_dir.mkdir(parents=True, exist_ok=True)
        self.badges_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "pl-knowledge-engine/0.1.0"})
        self.checkpoint = Checkpoint("image_download")
        self.metadata: list[dict[str, Any]] = []

    @retry(max_attempts=4, base_delay=1.0)
    def _fetch_bootstrap(self) -> dict[str, Any]:
        """Fetch the FPL bootstrap-static endpoint.

        Returns:
            Parsed JSON dict containing ``elements`` (players) and ``teams``.

        Raises:
            requests.HTTPError: On non-2xx response after retries.
        """
        resp = self.session.get(FPL_BOOTSTRAP_URL, timeout=15)
        resp.raise_for_status()
        return resp.json()

    @retry(max_attempts=4, base_delay=1.0)
    def _download_image(self, url: str, dest: Path) -> bool:
        """Download a single image from a URL and save to disk.

        Args:
            url: Direct image URL.
            dest: Local destination path (including filename).

        Returns:
            True if the image was downloaded, False if the URL returned 404.

        Raises:
            requests.HTTPError: On non-2xx, non-404 responses.
        """
        resp = self.session.get(url, timeout=10)
        if resp.status_code == 404:
            logger.debug("image not found (404)", url=url)
            return False
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        return True

    def _team_name_matches_focus(self, team_name: str) -> bool:
        """Check whether a team name matches one of the focus teams.

        Args:
            team_name: Team name string from the FPL API.

        Returns:
            True if the team is Aston Villa or Liverpool (case-insensitive).
        """
        name_lower = team_name.lower()
        return any(focus.lower() in name_lower for focus in FOCUS_TEAM_NAMES)

    def download_badges(self, teams_data: list[dict[str, Any]]) -> None:
        """Download team badge images for focus teams.

        Args:
            teams_data: List of team records from the FPL bootstrap ``teams`` key.
        """
        for team in teams_data:
            name = team.get("name", "")
            if not self._team_name_matches_focus(name):
                continue

            badge_code = team.get("code")
            if not badge_code:
                logger.warning("no badge code for team", team=name)
                continue

            item_id = f"badge_{badge_code}"
            if self.checkpoint.is_completed(item_id):
                logger.info("badge already downloaded, skipping", team=name)
                continue

            url = BADGE_URL.format(code=badge_code)
            dest = self.badges_dir / f"{_safe_filename(name)}.png"
            if self._download_image(url, dest):
                self.checkpoint.mark_completed(item_id)
                self.metadata.append(
                    {
                        "type": "team_badge",
                        "team": name,
                        "source_url": url,
                        "local_path": str(dest),
                        "source_type": "team_badge",
                        "modality": "image",
                    }
                )
                logger.info("badge downloaded", team=name)

    def download_player_photos(self, players_data: list[dict[str, Any]], teams_data: list[dict[str, Any]]) -> None:
        """Download headshots for all focus-team players.

        Args:
            players_data: List of player records from the FPL bootstrap ``elements`` key.
            teams_data: List of team records (used to resolve team IDs to names).
        """
        # Map team ID → team name
        team_id_to_name = {t["id"]: t["name"] for t in teams_data}

        for player in players_data:
            team_id = player.get("team")
            team_name = team_id_to_name.get(team_id, "")
            if not self._team_name_matches_focus(team_name):
                continue

            photo_code_raw = player.get("photo", "")
            # FPL uses e.g. "118748.jpg" — strip extension, prefix with "p"
            photo_code = photo_code_raw.replace(".jpg", "")
            if not photo_code:
                continue

            player_name = f"{player.get('first_name', '')} {player.get('second_name', '')}".strip()
            item_id = f"player_{photo_code}"

            if self.checkpoint.is_completed(item_id):
                logger.info("photo already downloaded, skipping", player=player_name)
                continue

            url = PLAYER_PHOTO_URL.format(code=photo_code)
            dest = self.players_dir / f"{_safe_filename(player_name)}.png"
            if self._download_image(url, dest):
                self.checkpoint.mark_completed(item_id)
                self.metadata.append(
                    {
                        "type": "player_photo",
                        "player": player_name,
                        "team": team_name,
                        "fpl_id": player.get("id"),
                        "source_url": url,
                        "local_path": str(dest),
                        "source_type": "player_image",
                        "modality": "image",
                    }
                )
                logger.info("photo downloaded", player=player_name, team=team_name)

    def save_metadata(self) -> None:
        """Write accumulated image metadata to ``images_metadata.json``."""
        meta_path = settings.raw_dir / "images" / "images_metadata.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)
        logger.info("image metadata saved", count=len(self.metadata))

    def run(self) -> None:
        """Execute the full image download pipeline."""
        logger.info("fetching fpl bootstrap")
        bootstrap = self._fetch_bootstrap()

        teams = bootstrap.get("teams", [])
        players = bootstrap.get("elements", [])

        logger.info("bootstrap fetched", teams=len(teams), players=len(players))

        self.download_badges(teams)
        self.download_player_photos(players, teams)
        self.save_metadata()

        logger.info(
            "image download complete",
            badges=sum(1 for m in self.metadata if m["type"] == "team_badge"),
            photos=sum(1 for m in self.metadata if m["type"] == "player_photo"),
        )


def run_image_download() -> None:
    """Orchestrate the full image download pipeline."""
    downloader = ImageDownloader()
    downloader.run()
