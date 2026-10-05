"""Validate prepared text and media; report the target honestly without embedding."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.corpus.acceptance import accept_corpus


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--document-budget", type=int, default=5000)
    parser.add_argument("--byte-budget", type=int, default=150_000_000)
    parser.add_argument("--require-target", action="store_true")
    args = parser.parse_args()
    report = accept_corpus(
        args.output, args.season, document_budget=args.document_budget, byte_budget=args.byte_budget
    )
    if not report["valid"]:
        raise SystemExit(
            "Corpus acceptance failed; inspect reports/multimodal/<season>/corpus.json"
        )
    print(
        f"Verified documents: {report['counts']['retrieval_documents']}; "
        f"original sources: {report['counts']['source_assets']}; "
        f"status: {report['status']}; no embeddings created"
    )
    if args.require_target and not report["counts"]["multimodal_target_met"]:
        raise SystemExit("The 2,500-document/four-modality target has not been met")


if __name__ == "__main__":
    main()
