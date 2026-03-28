"""
End-to-end pipeline runner.

Executes all stages in sequence or a single named stage.

Usage:
    python scripts/run_pipeline.py                  # run everything
    python scripts/run_pipeline.py --stage ingest   # stats ingestion only
    python scripts/run_pipeline.py --stage clean    # cleaning only
    python scripts/run_pipeline.py --stage agent1   # podcast transcript pipeline
    python scripts/run_pipeline.py --stage embed    # embedding only
    python scripts/run_pipeline.py --stage store    # ChromaDB load only
    python scripts/run_pipeline.py --stage graph    # Neo4j load only
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def run_ingest() -> None:
    from src.ingest.stats_api import run_stats_ingestion
    from src.ingest.fpl_data import run_fpl_ingestion
    logger.info("stage: ingest")
    run_stats_ingestion()
    run_fpl_ingestion()


def run_clean() -> None:
    from src.clean.stats_cleaner import run_stats_cleaning
    from src.clean.stats_summarizer import run_stats_summarization
    logger.info("stage: clean")
    run_stats_cleaning()
    run_stats_summarization()


def run_agent1() -> None:
    import json
    from config.settings import settings
    from src.ingest.youtube_search import run_youtube_search
    from src.ingest.transcripts import run_transcript_fetch
    from src.clean.transcript_cleaner import run_transcript_cleaning
    from src.clean.chunker import run_chunking

    logger.info("stage: agent1 (podcast transcripts)")
    manifest_path = settings.raw_dir / "transcripts" / "podcast_episodes.json"

    try:
        episodes = run_youtube_search()
    except Exception as exc:
        logger.error("youtube search failed", error=str(exc))
        episodes = json.loads(manifest_path.read_text()) if manifest_path.exists() else []

    if not episodes:
        logger.error("no episodes found — skipping agent1 stages")
        return

    run_transcript_fetch(episodes)
    run_transcript_cleaning(episodes)
    run_chunking(episodes)


def run_embed() -> None:
    from src.clean.stats_summarizer import run_stats_summarization
    from src.embed.embedder import run_embedding_pipeline
    logger.info("stage: embed")
    run_stats_summarization()
    run_embedding_pipeline()


def run_store() -> None:
    from src.store.chroma_store import ChromaStore
    logger.info("stage: store (ChromaDB)")
    store = ChromaStore()
    store.load_all_embedded()
    for name, count in store.collection_counts().items():
        logger.info("collection", name=name, count=count)


def run_graph() -> None:
    import subprocess
    logger.info("stage: graph (Neo4j)")
    subprocess.run([sys.executable, "scripts/seed_graph.py"], check=True)
    subprocess.run([sys.executable, "scripts/load_graph.py"], check=True)


STAGES = {
    "ingest": run_ingest,
    "clean": run_clean,
    "agent1": run_agent1,
    "embed": run_embed,
    "store": run_store,
    "graph": run_graph,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="PL Knowledge Engine pipeline runner")
    parser.add_argument(
        "--stage",
        choices=list(STAGES.keys()),
        help="Run only this stage (omit to run all stages in sequence)",
    )
    args = parser.parse_args()

    if args.stage:
        logger.info("running single stage", stage=args.stage)
        STAGES[args.stage]()
    else:
        logger.info("running full pipeline")
        for stage_name, stage_fn in STAGES.items():
            try:
                stage_fn()
                logger.info("stage complete", stage=stage_name)
            except Exception as exc:
                logger.error("stage failed", stage=stage_name, error=str(exc))

    logger.info("pipeline done")


if __name__ == "__main__":
    main()
