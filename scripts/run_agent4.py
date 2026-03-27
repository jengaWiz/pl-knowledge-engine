"""
Agent 4 runner: Video Highlight Download.

Independent of Agents 1 and 2 — reads cleaned match data from Phase 2.
Requires ffmpeg for trimming clips longer than 120 seconds.

Usage:
    python scripts/run_agent4.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger, setup_logging
from src.ingest.video_download import run_video_download

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    """Run the Agent 4 video highlight download pipeline."""
    logger.info("agent 4 starting: video highlight download")
    try:
        run_video_download()
        logger.info("agent 4 complete")
    except Exception as exc:
        logger.error("agent 4 failed", error=str(exc))
        raise


if __name__ == "__main__":
    main()
