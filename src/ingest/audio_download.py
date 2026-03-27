"""
Audio download agent.

Downloads podcast episode audio from YouTube using yt-dlp, converting to
128 kbps MP3. Reads the episode manifest produced by Agent 1.

Output: data/raw/audio/{youtube_id}.mp3
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import yt_dlp

from config.settings import settings
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger

logger = get_logger(__name__)

DOWNLOAD_DELAY_SECONDS = 5


class AudioDownloader:
    """Downloads podcast episodes as MP3 files using yt-dlp.

    Attributes:
        audio_dir: Destination directory for downloaded MP3 files.
        checkpoint: Stage checkpoint for resumability.
    """

    def __init__(self) -> None:
        """Initialise audio directory and checkpoint."""
        self.audio_dir = settings.raw_dir / "audio"
        self.checkpoint = Checkpoint("audio_download")
        self.audio_dir.mkdir(parents=True, exist_ok=True)

    def _ydl_opts(self) -> dict[str, Any]:
        """Build yt-dlp options for 128 kbps MP3 extraction."""
        return {
            "format": "bestaudio/best",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "128",
                }
            ],
            "outtmpl": str(self.audio_dir / "%(id)s.%(ext)s"),
            "quiet": True,
        }

    def download_episode(self, episode: dict[str, Any]) -> bool:
        """Download audio for a single episode.

        Skips episodes already marked completed in the checkpoint.

        Args:
            episode: Episode metadata dict containing ``youtube_id``.

        Returns:
            True if the episode was downloaded (or already done).
            False if the download failed.
        """
        vid_id = episode["youtube_id"]

        if self.checkpoint.is_completed(vid_id):
            logger.info("audio already downloaded, skipping", youtube_id=vid_id)
            return True

        url = f"https://www.youtube.com/watch?v={vid_id}"
        logger.info(
            "downloading audio",
            youtube_id=vid_id,
            title=episode.get("title", "")[:60],
        )

        try:
            with yt_dlp.YoutubeDL(self._ydl_opts()) as ydl:
                ydl.download([url])
            self.checkpoint.mark_completed(vid_id)
            logger.info("audio downloaded", youtube_id=vid_id)
            return True
        except yt_dlp.utils.DownloadError as exc:
            logger.warning(
                "audio download failed",
                youtube_id=vid_id,
                error=str(exc),
            )
            return False

    def download_all(self, episodes: list[dict[str, Any]]) -> dict[str, bool]:
        """Download audio for all episodes.

        Adds a delay between downloads to be kind to YouTube's servers.

        Args:
            episodes: Episode manifest list from Agent 1.

        Returns:
            Dict mapping youtube_id → True (success) / False (failure).
        """
        results: dict[str, bool] = {}
        total = len(episodes)

        for i, episode in enumerate(episodes, start=1):
            vid_id = episode["youtube_id"]

            if self.checkpoint.is_completed(vid_id):
                results[vid_id] = True
                continue

            logger.info("downloading", progress=f"{i}/{total}")
            results[vid_id] = self.download_episode(episode)

            if i < total:
                time.sleep(DOWNLOAD_DELAY_SECONDS)

        logger.info(
            "audio download complete",
            total=total,
            success=sum(v for v in results.values()),
        )
        return results


def run_audio_download(episodes: list[dict[str, Any]]) -> dict[str, bool]:
    """Orchestrate audio download for all manifest episodes.

    Args:
        episodes: Episode manifest from Agent 1.

    Returns:
        Dict mapping youtube_id → download success/failure.
    """
    downloader = AudioDownloader()
    return downloader.download_all(episodes)
