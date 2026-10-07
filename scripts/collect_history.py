"""Collect pinned historical PL seasons and prepare source-backed match documents."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.ingest.match_history import collect_history


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--document-budget", type=int, default=2000)
    parser.add_argument("--byte-budget", type=int, default=2_000_000)
    args = parser.parse_args()
    report = collect_history(
        args.output, document_budget=args.document_budget, byte_budget=args.byte_budget
    )
    if not report["valid"]:
        raise SystemExit("Historical collection failed; inspect reports/history/corpus.json")
    print(
        f"Verified {report['counts']['retrieval_documents']} historical match documents "
        f"across {len(report['coverage'])} seasons; no embeddings created"
    )


if __name__ == "__main__":
    main()
