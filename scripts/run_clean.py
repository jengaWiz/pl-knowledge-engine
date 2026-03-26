"""
Cleaning pipeline runner.

Loads raw ingested data from ``data/raw/`` and writes cleaned outputs
to ``data/cleaned/``.

Usage:
    python scripts/run_clean.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger, setup_logging
from src.clean.stats_cleaner import run_stats_cleaning

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    """Entry point for the cleaning pipeline.

    Runs the stats cleaner which normalizes, deduplicates, and enriches
    raw match and player data.
    """
    logger.info("starting cleaning pipeline")

    try:
        logger.info("stage: stats cleaner")
        results = run_stats_cleaning()
        for name, df in results.items():
            logger.info("cleaned dataset ready", name=name, rows=len(df))
        logger.info("stage complete: stats cleaner")
    except Exception as exc:
        logger.error("stage failed: stats cleaner", error=str(exc))
        raise

    logger.info("cleaning pipeline finished")


if __name__ == "__main__":
    main()
