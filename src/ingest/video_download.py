"""
Video highlight downloader.

Searches the Premier League's official YouTube channel for match highlight
clips and downloads them using yt-dlp. Clips longer than 120 seconds are
trimmed to that limit via ffmpeg for Gemini Embedding 2 compatibility.

Output:
    data/raw/video/{match_id}_highlights.mp4
    data/raw/video/video_metadata.json
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
from googleapiclient.discovery import build

from config.settings import settings
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)

# Premier League official YouTube channel ID
PL_CHANNEL_ID = "UCqj6ZxQ2fVi9C2zB4jGV_lQ"
MIN_HIGHLIGHT_SECONDS = 60    # Minimum plausible highlight length
MAX_HIGHLIGHT_SECONDS = 600   # Skip live streams / full matches
GEMINI_MAX_SECONDS = 120      # Trim to this for Gemini embedding


def _check_ffmpeg() -> None:
    """Raise RuntimeError if ffmpeg is not on the system PATH."""
    if not shutil.which("ffmpeg"):
        raise RuntimeError(
            "ffmpeg is required for video trimming. "
            "Install with: brew install ffmpeg  (macOS)"
        )


def _parse_iso8601_duration(duration: str) -> int:
    """Convert an ISO 8601 duration string to seconds.

    Args:
        duration: Duration string, e.g. ``PT3M45S``.

    Returns:
        Total seconds as an integer.
    """
    import re
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not match:
        return 0
    h = int(match.group(1) or 0)
    m = int(match.group(2) or 0)
    s = int(match.group(3) or 0)
    return h * 3600 + m * 60 + s


class VideoHighlightDownloader:
    """Searches and downloads match highlight clips from YouTube.

    Attributes:
        youtube: Authenticated YouTube Data API v3 service.
        video_dir: Output directory for MP4 files.
        checkpoint: Tracks which matches have been downloaded.
        metadata: Accumulated video metadata records.
    """

    def __init__(self) -> None:
        """Initialise API client and output directories."""
        _check_ffmpeg()
        self.youtube = build(
            "youtube", "v3", developerKey=settings.youtube_api_key
        )
        self.video_dir = settings.raw_dir / "video"
        self.video_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint = Checkpoint("video_download")
        self.metadata: list[dict[str, Any]] = []

    @retry(max_attempts=4, base_delay=1.0)
    def _search_highlights(
        self, home_team: str, away_team: str
    ) -> list[dict[str, Any]]:
        """Search the PL channel for highlights of a specific match.

        Args:
            home_team: Home team name for the search query.
            away_team: Away team name for the search query.

        Returns:
            List of search result items from the YouTube API.
        """
        query = f"{home_team} vs {away_team} highlights"
        response = (
            self.youtube.search()
            .list(
                channelId=PL_CHANNEL_ID,
                q=query,
                type="video",
                maxResults=3,
                part="id,snippet",
            )
            .execute()
        )
        return response.get("items", [])

    @retry(max_attempts=4, base_delay=1.0)
    def _get_video_duration(self, video_id: str) -> int:
        """Retrieve duration in seconds for a single YouTube video.

        Args:
            video_id: YouTube video ID string.

        Returns:
            Duration in seconds.
        """
        response = (
            self.youtube.videos()
            .list(id=video_id, part="contentDetails")
            .execute()
        )
        items = response.get("items", [])
        if not items:
            return 0
        return _parse_iso8601_duration(
            items[0]["contentDetails"].get("duration", "PT0S")
        )

    def _download_video(self, youtube_id: str, dest: Path) -> bool:
        """Download a video using yt-dlp at ≤720p.

        Args:
            youtube_id: YouTube video ID.
            dest: Destination path stem (without extension; yt-dlp adds .mp4).

        Returns:
            True if download succeeded.
        """
        import yt_dlp

        url = f"https://www.youtube.com/watch?v={youtube_id}"
        opts = {
            "format": "best[height<=720][ext=mp4]/best[height<=720]",
            "outtmpl": str(dest),
            "quiet": True,
        }
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
            return True
        except Exception as exc:
            logger.error("video download failed", youtube_id=youtube_id, error=str(exc))
            return False

    def _trim_video(self, src: Path, dest: Path, max_seconds: int) -> None:
        """Trim a video file to ``max_seconds`` using ffmpeg.

        Args:
            src: Source MP4 path.
            dest: Destination trimmed MP4 path.
            max_seconds: Maximum length in seconds.
        """
        cmd = [
            "ffmpeg", "-y", "-i", str(src),
            "-t", str(max_seconds),
            "-c", "copy", str(dest),
        ]
        result = subprocess.run(cmd, capture_output=True)
        if result.returncode != 0:
            logger.error("ffmpeg trim failed", stderr=result.stderr.decode())
        else:
            src.unlink()  # Remove untrimmed file
            logger.info("video trimmed", seconds=max_seconds, dest=str(dest))

    def download_match_highlights(self, match: dict[str, Any]) -> bool:
        """Find and download highlights for one match.

        Args:
            match: Match record dict with ``id``, ``home_team``, ``away_team``
                fields (as returned from cleaned stats data).

        Returns:
            True if a highlight clip was found and downloaded.
        """
        match_id = str(match.get("id", ""))
        home = match.get("home_team", {})
        away = match.get("away_team", {})

        # Handle both flat string and nested dict team formats
        home_name = home if isinstance(home, str) else home.get("name", "")
        away_name = away if isinstance(away, str) else away.get("name", "")

        if not home_name or not away_name:
            logger.warning("missing team names in match record", match_id=match_id)
            return False

        logger.info("searching highlights", home=home_name, away=away_name)
        results = self._search_highlights(home_name, away_name)

        for result in results:
            vid_id = result.get("id", {}).get("videoId")
            if not vid_id:
                continue
            duration = self._get_video_duration(vid_id)
            if not (MIN_HIGHLIGHT_SECONDS <= duration <= MAX_HIGHLIGHT_SECONDS):
                logger.debug(
                    "highlight filtered by duration",
                    video_id=vid_id,
                    duration=duration,
                )
                continue

            dest = self.video_dir / f"{match_id}_highlights.mp4"
            tmp = self.video_dir / f"{match_id}_highlights_full.mp4"

            if not self._download_video(vid_id, tmp):
                continue

            # Trim if needed
            if duration > GEMINI_MAX_SECONDS and tmp.exists():
                self._trim_video(tmp, dest, GEMINI_MAX_SECONDS)
                final_duration = GEMINI_MAX_SECONDS
            else:
                if tmp.exists():
                    tmp.rename(dest)
                final_duration = duration

            self.metadata.append(
                {
                    "match_id": match_id,
                    "youtube_id": vid_id,
                    "title": result.get("snippet", {}).get("title", ""),
                    "home_team": home_name,
                    "away_team": away_name,
                    "duration_seconds": final_duration,
                    "local_path": str(dest),
                    "source_type": "video_highlight",
                    "modality": "video",
                }
            )
            logger.info(
                "highlight downloaded",
                match_id=match_id,
                youtube_id=vid_id,
                duration=final_duration,
            )
            return True

        logger.warning("no suitable highlight found", home=home_name, away=away_name)
        return False

    def save_metadata(self) -> None:
        """Persist video metadata to ``data/raw/video/video_metadata.json``."""
        meta_path = self.video_dir / "video_metadata.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)
        logger.info("video metadata saved", count=len(self.metadata))

    def run(self, matches_csv: Path) -> None:
        """Download highlights for all focus-team matches.

        Reads the cleaned match CSV, iterates over each match, and downloads
        a highlight clip if one is available.

        Args:
            matches_csv: Path to ``data/cleaned/stats/matches.csv``.
        """
        if not matches_csv.exists():
            logger.error("matches csv not found", path=str(matches_csv))
            return

        df = pd.read_csv(matches_csv)
        logger.info("loaded matches", total=len(df))

        for _, row in df.iterrows():
            match_id = str(row.get("id", ""))
            if not match_id:
                continue
            if self.checkpoint.is_completed(match_id):
                logger.info("already downloaded, skipping", match_id=match_id)
                continue
            if self.download_match_highlights(row.to_dict()):
                self.checkpoint.mark_completed(match_id)

        self.save_metadata()
        logger.info("video download complete", total=len(self.metadata))


def run_video_download() -> None:
    """Orchestrate video highlight downloads from cleaned match data."""
    matches_csv = settings.cleaned_dir / "stats" / "matches.csv"
    downloader = VideoHighlightDownloader()
    downloader.run(matches_csv)
