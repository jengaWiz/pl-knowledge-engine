"""
YouTube transcript extractor.

Fetches auto-generated or manual captions from YouTube videos using the
youtube-transcript-api library. Saves both a plain-text concatenation and a
timestamped segment JSON for each episode.

Output per episode:
    data/raw/transcripts/{youtube_id}_raw.txt
    data/raw/transcripts/{youtube_id}_segments.json
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from youtube_transcript_api import (
    YouTubeTranscriptApi,
    NoTranscriptFound,
    TranscriptsDisabled,
)

from config.settings import settings
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TranscriptFetcher:
    """Fetches and saves transcripts for a list of YouTube episodes.

    Attributes:
        api: YouTubeTranscriptApi instance for fetching captions.
        transcript_dir: Directory where raw transcript files are saved.
        checkpoint: Tracks which video IDs have already been processed.
    """

    def __init__(self) -> None:
        """Initialize the transcript fetcher with paths from settings."""
        self.api = YouTubeTranscriptApi()
        self.transcript_dir = settings.raw_dir / "transcripts"
        self.transcript_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint = Checkpoint("transcript_fetch")

    def _fetch_transcript(self, video_id: str) -> list[dict[str, Any]] | None:
        """Attempt to fetch transcript segments for a video.

        Tries English manual captions first; falls back to auto-generated
        English captions. Returns None if no captions are available.

        Args:
            video_id: YouTube video ID string.

        Returns:
            List of segment dicts with ``text``, ``start``, and ``duration``
            keys, or None if transcripts are unavailable.
        """
        try:
            transcript_list = self.api.list_transcripts(video_id)

            # Prefer manual English captions
            try:
                transcript = transcript_list.find_manually_created_transcript(["en"])
                logger.info("using manual transcript", video_id=video_id)
            except NoTranscriptFound:
                # Fall back to auto-generated
                transcript = transcript_list.find_generated_transcript(["en"])
                logger.info("using auto-generated transcript", video_id=video_id)

            return transcript.fetch()

        except TranscriptsDisabled:
            logger.warning("transcripts disabled for video", video_id=video_id)
            return None
        except NoTranscriptFound:
            logger.warning("no transcript found for video", video_id=video_id)
            return None
        except Exception as exc:
            logger.error(
                "unexpected error fetching transcript",
                video_id=video_id,
                error=str(exc),
            )
            return None

    def _save_transcript(
        self, video_id: str, segments: list[dict[str, Any]]
    ) -> None:
        """Save transcript segments to disk as raw text and JSON.

        Args:
            video_id: YouTube video ID used as the filename prefix.
            segments: List of timestamped segment dicts from the API.
        """
        # Plain-text concatenation
        raw_text = " ".join(seg.get("text", "") for seg in segments)
        raw_path = self.transcript_dir / f"{video_id}_raw.txt"
        raw_path.write_text(raw_text, encoding="utf-8")

        # Timestamped JSON
        segments_path = self.transcript_dir / f"{video_id}_segments.json"
        with open(segments_path, "w", encoding="utf-8") as f:
            json.dump(segments, f, indent=2, ensure_ascii=False)

        logger.info(
            "transcript saved",
            video_id=video_id,
            segments=len(segments),
            chars=len(raw_text),
        )

    def fetch_all(self, episodes: list[dict[str, Any]]) -> dict[str, bool]:
        """Fetch transcripts for all episodes in the manifest.

        Args:
            episodes: List of episode metadata dicts (must include
                ``youtube_id`` key).

        Returns:
            Dict mapping each ``youtube_id`` to True (success) or False
            (unavailable/error).
        """
        results: dict[str, bool] = {}

        for episode in episodes:
            vid_id = episode["youtube_id"]

            if self.checkpoint.is_completed(vid_id):
                logger.info("transcript already fetched, skipping", video_id=vid_id)
                results[vid_id] = True
                continue

            segments = self._fetch_transcript(vid_id)
            if segments is not None:
                self._save_transcript(vid_id, segments)
                self.checkpoint.mark_completed(vid_id)
                results[vid_id] = True
            else:
                results[vid_id] = False

        success = sum(v for v in results.values())
        logger.info(
            "transcript fetch complete",
            total=len(results),
            success=success,
            skipped=len(results) - success,
        )
        return results


def run_transcript_fetch(episodes: list[dict[str, Any]]) -> dict[str, bool]:
    """Orchestrate transcript fetching for a list of episodes.

    Args:
        episodes: List of episode metadata dicts containing ``youtube_id``.

    Returns:
        Dict mapping each youtube_id to True (fetched) or False (unavailable).
    """
    fetcher = TranscriptFetcher()
    return fetcher.fetch_all(episodes)
