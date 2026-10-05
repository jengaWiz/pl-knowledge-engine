"""Extract traceable media retrieval units without embedding/provider calls."""

import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.corpus.media_extraction import extract_media


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--segment-seconds", type=int, default=20)
    parser.add_argument("--byte-budget", type=int, default=100_000_000)
    parser.add_argument("--document-budget", type=int, default=500)
    args = parser.parse_args()
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        parser.error("Media extraction requires FFmpeg and ffprobe")
    report = extract_media(
        args.output,
        segment_seconds=args.segment_seconds,
        byte_budget=args.byte_budget,
        document_budget=args.document_budget,
    )
    if not report["valid"]:
        raise SystemExit("Extraction failed; inspect reports/multimodal/extraction.json")
    print(
        f"Prepared media documents: {report['counts']['retrieval_documents']}; "
        f"original sources: {report['counts']['source_assets']}; no embeddings created"
    )


if __name__ == "__main__":
    main()
