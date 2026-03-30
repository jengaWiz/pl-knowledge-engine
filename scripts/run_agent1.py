"""
Agent 1 runner: YouTube Search → Transcript Fetch → Transcript Cleaning → Chunking.

This script orchestrates the full Agent 1 pipeline in sequence.
It must be run before Agent 2, which reads the episode manifest produced here.

Usage:
    python scripts/run_agent1.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger, setup_logging
from src.ingest.youtube_search import run_youtube_search
from src.ingest.transcripts import run_transcript_fetch
from src.clean.transcript_cleaner import run_transcript_cleaning
from src.clean.chunker import run_chunking
from config.settings import settings

setup_logging()
logger = get_logger(__name__)

MANIFEST_PATH = settings.raw_dir / "transcripts" / "podcast_episodes.json"


def _load_manifest() -> list[dict]:
    """Load the episode manifest if it already exists on disk.

    Returns:
        Parsed list of episode dicts, or empty list if file doesn't exist.
    """
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            return json.load(f)
    return []


def main() -> None:
    """Run the full Agent 1 pipeline end-to-end.

    Stages:
    1. YouTube search — discover podcast episodes for both focus teams.
    2. Transcript fetch — download captions for each discovered episode.
    3. Transcript cleaning — fix punctuation/names (LLM or regex).
    4. Chunking — split cleaned text into embedding-ready chunks with metadata.
    """
    logger.info("agent 1 starting")

    # Stage 1: YouTube search
    try:
        logger.info("stage 1/4: youtube search")
        episodes = run_youtube_search()
        logger.info("stage 1 complete", episodes=len(episodes))
    except Exception as exc:
        logger.error("stage 1 failed", error=str(exc))
        # Try to continue with any manifest already on disk
        episodes = _load_manifest()
        if not episodes:
            logger.error("no episode manifest available — aborting")
            sys.exit(1)

    # Stage 2: Transcript fetch
    try:
        logger.info("stage 2/4: transcript fetch")
        results = run_transcript_fetch(episodes)
        fetched = sum(v for v in results.values())
        logger.info("stage 2 complete", fetched=fetched, total=len(results))
    except Exception as exc:
        logger.error("stage 2 failed", error=str(exc))

    # Stage 3: Transcript cleaning
    try:
        logger.info("stage 3/4: transcript cleaning")
        run_transcript_cleaning(episodes)
        logger.info("stage 3 complete")
    except Exception as exc:
        logger.error("stage 3 failed", error=str(exc))

    # Stage 4: Chunking
    try:
        logger.info("stage 4/4: chunking")
        chunks = run_chunking(episodes)
        logger.info("stage 4 complete", total_chunks=len(chunks))
    except Exception as exc:
        logger.error("stage 4 failed", error=str(exc))

    logger.info("agent 1 complete")


if __name__ == "__main__":
    main()
