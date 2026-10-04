"""Collect and validate the complete historical MVP match dataset."""

import argparse
import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from config.sources import load_sources
from src.ingest.historical_matches import MATCH_STATS, normalize_matches, validate_coverage
from src.ingest.source_download import atomic_write, download_source


def collect_matches(season: str, output: Path, *, refresh: bool = False) -> dict:
    source = next(source for source in load_sources(season) if source.id == "matches")
    raw_dir = output / "raw" / "mvp" / season
    report_path = output / "reports" / "mvp" / season / "matches.json"
    report = {"collected_at": datetime.now(UTC).isoformat(), "source": asdict(source)}
    try:
        content, cached = download_source(source, raw_dir / "matches.csv", refresh=refresh)
        records = normalize_matches(content, season, source.id)
        report.update(validate_coverage(records))
        report["cached"] = cached
        report["field_provenance"] = {
            "date": "Date",
            "home_team": "HomeTeam (explicit alias normalization)",
            "away_team": "AwayTeam (explicit alias normalization)",
            "home_score": "FTHG",
            "away_score": "FTAG",
            "result": "FTR",
            **{field: column for column, field in MATCH_STATS.items()},
            "id": "Derived from season and canonical home/away team pair",
            "season": "Pinned contract, independently checked against match dates",
        }
        if not report["valid"]:
            raise ValueError("Match completeness gate failed; see the coverage report")
        for record in records:
            record.update(
                source_url=source.url, source_sha256=source.sha256, source_revision=source.revision
            )
        normalized = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)
        atomic_write(output / "cleaned" / "mvp" / season / "matches.jsonl", normalized.encode())
        report["status"] = "complete"
    except (ValueError, requests.RequestException) as exc:
        report.update(valid=False, status="failed", error=str(exc))
        atomic_write(report_path, (json.dumps(report, indent=2) + "\n").encode())
        raise
    atomic_write(report_path, (json.dumps(report, indent=2) + "\n").encode())
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument(
        "--refresh", action="store_true", help="Refetch and validate the pinned source"
    )
    args = parser.parse_args()
    report = collect_matches(args.season, args.output, refresh=args.refresh)
    print(
        f"Validated {report['fixtures']} fixtures across {report['teams']} teams; "
        f"{'verified cache' if report['cached'] else 'public download'}"
    )
    print(f"Coverage report: {args.output / 'reports' / 'mvp' / args.season / 'matches.json'}")


if __name__ == "__main__":
    main()
