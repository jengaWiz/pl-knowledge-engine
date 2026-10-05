"""Prepare source-backed statistical documents without model/provider calls."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.corpus.statistical_text import prepare_text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--document-budget", type=int, default=5000)
    parser.add_argument("--byte-budget", type=int, default=10_000_000)
    args = parser.parse_args()
    report = prepare_text(
        args.output, args.season, document_budget=args.document_budget, byte_budget=args.byte_budget
    )
    if not report["valid"]:
        raise SystemExit("Text preparation failed; inspect reports/multimodal/<season>/text.json")
    print(
        f"Prepared {report['counts']['retrieval_documents']} statistical text documents; "
        "no embeddings created"
    )


if __name__ == "__main__":
    main()
