"""Collect reviewed public media, checking checksums, terms and bounded decoding."""

import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.ingest.commons_media import collect_media


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--byte-budget", type=int, default=50_000_000)
    args = parser.parse_args()
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        parser.error("This optional collector requires FFmpeg and ffprobe")
    report = collect_media(args.output, byte_budget=args.byte_budget)
    print(
        f"Verified original media assets: {report['counts']['source_assets']}; "
        f"failures: {len(report['failures'])}; no documents embedded"
    )
    if not report["valid"]:
        raise SystemExit("Media collection failed; inspect reports/multimodal/commons.json")


if __name__ == "__main__":
    main()
