"""
Ingestion pipeline runner.

Runs the full data ingestion stage: BallDontLie EPL stats API and
FPL-Core-Insights CSV download.

Usage:
    python scripts/run_ingest.py
"""
import sys
from pathlib import Path

# Ensure the project root is on the path so config/src imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger, setup_logging
from src.ingest.stats_api import run_stats_ingestion
from src.ingest.fpl_data import run_fpl_ingestion

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    """Entry point for the ingestion pipeline.

    Runs stats ingestion from BallDontLie followed by FPL data download.
    Logs success or failure for each stage independently so a partial failure
    does not prevent other stages from running.
    """
    logger.info("starting ingestion pipeline")

    try:
        logger.info("stage: balldontlie stats api")
        run_stats_ingestion()
        logger.info("stage complete: balldontlie stats api")
    except Exception as exc:
        logger.error("stage failed: balldontlie stats api", error=str(exc))

    try:
        logger.info("stage: fpl data download")
        run_fpl_ingestion()
        logger.info("stage complete: fpl data download")
    except Exception as exc:
        logger.error("stage failed: fpl data download", error=str(exc))

    logger.info("ingestion pipeline finished")


if __name__ == "__main__":
    main()
