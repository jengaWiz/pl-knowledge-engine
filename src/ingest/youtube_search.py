"""
YouTube Data API v3 search client.

Searches target podcast channels for Premier League episodes featuring
Aston Villa or Liverpool, then retrieves video duration metadata to filter
out short clips and live-stream archives.

Output: data/raw/transcripts/podcast_episodes.json
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from googleapiclient.discovery import build

from config.podcast_channels import PODCAST_CHANNELS
from config.settings import settings
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)

SEASON_START = "2025-08-01T00:00:00Z"
MIN_DURATION_SECONDS = 600    # 10 minutes — exclude short clips
MAX_DURATION_SECONDS = 10800  # 3 hours — exclude live streams


def _parse_iso8601_duration(duration: str) -> int:
    """Convert a YouTube ISO 8601 duration string to total seconds.

    Args:
        duration: ISO 8601 duration string, e.g. ``PT1H4M30S``.

    Returns:
        Total duration in seconds as an integer.
    """
    import re
    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    match = re.match(pattern, duration)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


class YouTubeSearchClient:
    """Searches YouTube channels for Premier League podcast episodes.

    Attributes:
        youtube: Authenticated YouTube Data API v3 service object.
        output_path: Destination path for the episode manifest JSON.
        checkpoint: Tracks which channels have already been searched.
    """

    def __init__(self) -> None:
        """Initialize the YouTube API client using credentials from settings."""
        self.youtube = build(
            "youtube", "v3", developerKey=settings.youtube_api_key
        )
        self.output_path = settings.raw_dir / "transcripts" / "podcast_episodes.json"
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint = Checkpoint("youtube_search")

    @retry(max_attempts=5, base_delay=1.0)
    def _search_channel(
        self, channel_id: str, keywords: list[str]
    ) -> list[dict[str, Any]]:
        """Search a single channel for videos matching the given keywords.

        Args:
            channel_id: YouTube channel ID (UC... format).
            keywords: List of search keywords to OR together.

        Returns:
            List of raw search result items from the YouTube API.
        """
        query = " OR ".join(keywords)
        response = (
            self.youtube.search()
            .list(
                channelId=channel_id,
                q=query,
                type="video",
                publishedAfter=SEASON_START,
                maxResults=50,
                part="id,snippet",
            )
            .execute()
        )
        return response.get("items", [])

    @retry(max_attempts=5, base_delay=1.0)
    def _get_video_details(self, video_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch full metadata (duration, title, description) for a list of video IDs.

        Args:
            video_ids: List of YouTube video IDs (up to 50 per request).

        Returns:
            List of video resource objects with ``contentDetails`` and ``snippet``.
        """
        response = (
            self.youtube.videos()
            .list(
                id=",".join(video_ids),
                part="contentDetails,snippet",
            )
            .execute()
        )
        return response.get("items", [])

    def _is_valid_episode(self, duration_seconds: int) -> bool:
        """Check whether a video duration falls within episode bounds.

        Args:
            duration_seconds: Video duration in seconds.

        Returns:
            True if the video is between ``MIN_DURATION_SECONDS`` and
            ``MAX_DURATION_SECONDS`` (inclusive).
        """
        return MIN_DURATION_SECONDS <= duration_seconds <= MAX_DURATION_SECONDS

    def search_all_channels(self) -> list[dict[str, Any]]:
        """Search every channel in ``PODCAST_CHANNELS`` and aggregate results.

        Skips channels that have already been processed (via checkpoint) or
        that have no channel ID configured yet.

        Returns:
            Deduplicated list of episode metadata dictionaries conforming to
            the episode manifest schema.
        """
        all_episodes: dict[str, dict[str, Any]] = {}

        # Load any previously discovered episodes from disk
        if self.output_path.exists():
            with open(self.output_path, encoding="utf-8") as f:
                for ep in json.load(f):
                    all_episodes[ep["youtube_id"]] = ep

        for channel in PODCAST_CHANNELS:
            if not channel.youtube_channel_id:
                logger.warning(
                    "skipping channel — no channel ID configured",
                    channel=channel.name,
                )
                continue

            if self.checkpoint.is_completed(channel.youtube_channel_id):
                logger.info(
                    "channel already searched, skipping",
                    channel=channel.name,
                )
                continue

            logger.info("searching channel", channel=channel.name)
            results = self._search_channel(
                channel.youtube_channel_id, channel.search_keywords
            )

            if not results:
                self.checkpoint.mark_completed(channel.youtube_channel_id)
                continue

            # Batch-fetch video details for duration filtering
            video_ids = [r["id"]["videoId"] for r in results if "videoId" in r.get("id", {})]
            if not video_ids:
                self.checkpoint.mark_completed(channel.youtube_channel_id)
                continue

            details = self._get_video_details(video_ids)

            for video in details:
                vid_id = video["id"]
                snippet = video.get("snippet", {})
                content = video.get("contentDetails", {})
                duration_str = content.get("duration", "PT0S")
                duration_seconds = _parse_iso8601_duration(duration_str)

                if not self._is_valid_episode(duration_seconds):
                    logger.debug(
                        "filtered out by duration",
                        video_id=vid_id,
                        duration_seconds=duration_seconds,
                    )
                    continue

                episode = {
                    "youtube_id": vid_id,
                    "title": snippet.get("title", ""),
                    "channel": channel.name,
                    "published_at": snippet.get("publishedAt", ""),
                    "duration_seconds": duration_seconds,
                    "description": snippet.get("description", "")[:500],
                }
                all_episodes[vid_id] = episode
                logger.info(
                    "found episode",
                    channel=channel.name,
                    title=episode["title"][:60],
                    duration_seconds=duration_seconds,
                )

            self.checkpoint.mark_completed(channel.youtube_channel_id)
            logger.info(
                "channel search complete",
                channel=channel.name,
                total_episodes=len(all_episodes),
            )

        return list(all_episodes.values())

    def save_manifest(self, episodes: list[dict[str, Any]]) -> None:
        """Persist the episode list to the manifest JSON file.

        Args:
            episodes: List of episode metadata dictionaries to save.
        """
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(episodes, f, indent=2, ensure_ascii=False)
        logger.info("saved episode manifest", count=len(episodes), path=str(self.output_path))


def run_youtube_search() -> list[dict[str, Any]]:
    """Orchestrate a full YouTube channel search and persist results.

    Returns:
        List of discovered podcast episode metadata dictionaries.
    """
    client = YouTubeSearchClient()
    episodes = client.search_all_channels()
    client.save_manifest(episodes)
    logger.info("youtube search complete", total_episodes=len(episodes))
    return episodes
