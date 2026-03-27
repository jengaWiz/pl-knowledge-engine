"""
Agent 3 runner: Image Download (player photos + team badges).

Independent of Agents 1 and 2 — only requires cleaned stats data from Phase 2.

Usage:
    python scripts/run_agent3.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger, setup_logging
from src.ingest.images import run_image_download

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    """Run the Agent 3 image download pipeline."""
    logger.info("agent 3 starting: image download")
    try:
        run_image_download()
        logger.info("agent 3 complete")
    except Exception as exc:
        logger.error("agent 3 failed", error=str(exc))
        raise


if __name__ == "__main__":
    main()
