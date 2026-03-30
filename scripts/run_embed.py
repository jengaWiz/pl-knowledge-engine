"""
Embedding pipeline runner.

Runs stats summarization then embeds all modalities via Gemini Embedding 2.

Usage:
    python scripts/run_embed.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger, setup_logging
from src.clean.stats_summarizer import run_stats_summarization
from src.embed.embedder import run_embedding_pipeline

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    logger.info("embed pipeline starting")

    logger.info("step 1/2: stats summarization")
    summaries = run_stats_summarization()
    logger.info(
        "stats summarization complete",
        matches=len(summaries["matches"]),
        players=len(summaries["players"]),
    )

    logger.info("step 2/2: embedding all modalities")
    results = run_embedding_pipeline()
    logger.info("embedding complete", results=results)


if __name__ == "__main__":
    main()
