"""
Vector store loader.

Loads all embedded JSONL files into ChromaDB and reports collection counts.

Usage:
    python scripts/run_store.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger, setup_logging
from src.store.chroma_store import ChromaStore

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    logger.info("store pipeline starting")

    store = ChromaStore()
    results = store.load_all_embedded()
    logger.info("vectors loaded", results=results)

    counts = store.collection_counts()
    for collection, count in counts.items():
        logger.info("collection count", collection=collection, count=count)


if __name__ == "__main__":
    main()
