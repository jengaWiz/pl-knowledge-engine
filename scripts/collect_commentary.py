"""Collect optional public commentary metadata, reporting unavailable sources."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.ingest.commentary import collect_commentary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    report = collect_commentary(args.season, args.output, manifest=args.manifest)
    print(f"External metadata: {report['coverage']}; fallback: {report['fallback']}")


if __name__ == "__main__":
    main()
