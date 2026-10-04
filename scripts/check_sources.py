"""Download and validate MVP source contracts without API keys or database access."""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from config.sources import inspect_source, load_sources

MAX_BYTES = 20_000_000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    args = parser.parse_args()
    reports = []
    raw_dir = args.output / "raw" / "source_checks"
    raw_dir.mkdir(parents=True, exist_ok=True)
    with requests.Session() as session:
        session.headers["User-Agent"] = "PL-Knowledge-Engine/0.1 (public dataset validation)"
        for source in load_sources(args.season):
            with session.get(source.url, timeout=(10, 30), stream=True) as response:
                response.raise_for_status()
                chunks = []
                size = 0
                for chunk in response.iter_content(65536):
                    size += len(chunk)
                    if size > MAX_BYTES:
                        raise ValueError(f"{source.id}: source exceeds {MAX_BYTES} bytes")
                    chunks.append(chunk)
            content = b"".join(chunks)
            report = inspect_source(source, content)
            (raw_dir / f"{source.id}.csv").write_bytes(content)
            reports.append(report)
            print(f"{source.id}: {report['rows']} rows; schema and checksum verified")
    report_dir = args.output / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "source_checks.json").write_text(
        json.dumps(
            {
                "checked_at": datetime.now(UTC).isoformat(),
                "season": args.season,
                "sources": reports,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
