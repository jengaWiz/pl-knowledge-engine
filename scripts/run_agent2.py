"""
Agent 2 runner: Audio Download → Audio Segmentation.

Reads the episode manifest from Agent 1 and downloads/segments audio.
Requires ffmpeg on the system PATH.

Usage:
    python scripts/run_agent2.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger, setup_logging
from src.ingest.audio_download import run_audio_download
from src.clean.audio_segmenter import run_audio_segmentation
from config.settings import settings

setup_logging()
logger = get_logger(__name__)

MANIFEST_PATH = settings.raw_dir / "transcripts" / "podcast_episodes.json"


def main() -> None:
    """Run the Agent 2 pipeline: audio download then segmentation."""
    if not MANIFEST_PATH.exists():
        logger.error(
            "episode manifest not found — run Agent 1 first",
            path=str(MANIFEST_PATH),
        )
        sys.exit(1)

    with open(MANIFEST_PATH, encoding="utf-8") as f:
        episodes = json.load(f)

    logger.info("agent 2 starting", episodes=len(episodes))

    try:
        logger.info("stage 1/2: audio download")
        run_audio_download(episodes)
        logger.info("stage 1 complete")
    except Exception as exc:
        logger.error("stage 1 failed", error=str(exc))

    try:
        logger.info("stage 2/2: audio segmentation")
        segments = run_audio_segmentation(episodes)
        logger.info("stage 2 complete", segments=len(segments))
    except Exception as exc:
        logger.error("stage 2 failed", error=str(exc))

    logger.info("agent 2 complete")


if __name__ == "__main__":
    main()
