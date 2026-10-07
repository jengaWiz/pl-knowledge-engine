"""Index accepted structured text with the local ONNX model, without Gemini calls."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.store.evidence_index import build_index


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--season", default=settings.season)
    args = parser.parse_args()
    report = build_index(args.output, args.season)
    print(
        f"Indexed {report['indexed_records']} records / {report['unique_documents']} unique "
        f"documents; reused {report['reused_batches']} batches; local ONNX, no Gemini calls"
    )


if __name__ == "__main__":
    main()
