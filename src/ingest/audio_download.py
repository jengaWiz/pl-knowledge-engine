"""
Audio download agent.

Downloads full podcast episode audio from YouTube using yt-dlp.
Reads the episode manifest written by Agent 1.

Output: data/raw/audio/{youtube_id}.mp3
"""
from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Any

import yt_dlp

from config.settings import settings
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger

logger = get_logger(__name__)

DOWNLOAD_DELAY_SECONDS = 5
BYTES_PER_HOUR_128KBPS = 57_600_000  # ~57.6 MB/hour at 128 kbps


def _check_ffmpeg() -> None:
    """Verify ffmpeg is installed on the system.

    Raises:
        RuntimeError: If ffmpeg is not found on the PATH.
    """
    if not shutil.which("ffmpeg"):
        raise RuntimeError(
            "ffmpeg is not installed or not on PATH. "
            "Install with: brew install ffmpeg  (macOS) "
            "or: sudo apt install ffmpeg  (Ubuntu)"
        )


class AudioDownloader:
    """Downloads podcast audio files using yt-dlp.

    Attributes:
        audio_dir: Destination directory for downloaded MP3 files.
        checkpoint: Tracks which episodes have been downloaded.
    """

    def __init__(self) -> None:
        """Set up the downloader; verifies ffmpeg is available."""
        _check_ffmpeg()
        self.audio_dir = settings.raw_dir / "audio"
        self.audio_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint = Checkpoint("audio_download")

    def _ydl_opts(self) -> dict[str, Any]:
        """Build yt-dlp options for audio-only MP3 download.

        Returns:
            Dict of yt-dlp configuration options.
        """
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
            "no_warnings": True,
        }

    def download_one(self, episode: dict[str, Any]) -> bool:
        """Download the audio for a single episode.

        Args:
            episode: Episode metadata dict (must contain ``youtube_id``
                and ``duration_seconds``).

        Returns:
            True if download succeeded, False on error.
        """
        vid_id = episode["youtube_id"]
        url = f"https://www.youtube.com/watch?v={vid_id}"
        est_mb = (episode.get("duration_seconds", 0) / 3600) * (BYTES_PER_HOUR_128KBPS / 1_000_000)

        logger.info(
            "downloading audio",
            youtube_id=vid_id,
            title=episode.get("title", "")[:60],
            estimated_mb=round(est_mb, 1),
        )

        try:
            with yt_dlp.YoutubeDL(self._ydl_opts()) as ydl:
                ydl.download([url])
            logger.info("audio downloaded", youtube_id=vid_id)
            return True
        except Exception as exc:
            logger.error("audio download failed", youtube_id=vid_id, error=str(exc))
            return False

    def download_all(self, episodes: list[dict[str, Any]]) -> None:
        """Download audio for all episodes in the manifest.

        Adds a delay between downloads to be respectful of YouTube's servers.

        Args:
            episodes: List of episode metadata dicts.
        """
        total = len(episodes)
        for i, episode in enumerate(episodes, start=1):
            vid_id = episode["youtube_id"]

            if self.checkpoint.is_completed(vid_id):
                logger.info("audio already downloaded, skipping", youtube_id=vid_id)
                continue

            logger.info("downloading", progress=f"{i}/{total}")
            if self.download_one(episode):
                self.checkpoint.mark_completed(vid_id)

            if i < total:
                logger.info("rate-limit delay", seconds=DOWNLOAD_DELAY_SECONDS)
                time.sleep(DOWNLOAD_DELAY_SECONDS)

        logger.info("audio download complete", total=total)


def run_audio_download(episodes: list[dict[str, Any]]) -> None:
    """Orchestrate audio download for all manifest episodes.

    Args:
        episodes: Episode manifest from Agent 1.
    """
    downloader = AudioDownloader()
    downloader.download_all(episodes)
