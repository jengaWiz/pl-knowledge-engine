"""Collect archived focus-team players and reconcile match-level appearances."""

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from config.sources import load_sources
from src.ingest.historical_players import build_player_corpus, source_rows
from src.ingest.source_download import atomic_write, download_source


def collect_players(season: str, output: Path, *, refresh: bool = False) -> dict:
    report_path = output / "reports" / "mvp" / season / "players.json"
    report = {"collected_at": datetime.now(UTC).isoformat(), "season": season}
    try:
        match_report = json.loads(
            (output / "reports" / "mvp" / season / "matches.json").read_text()
        )
        if not match_report.get("valid"):
            raise ValueError("Collect and validate matches before collecting players")
        canonical = [
            json.loads(line)
            for line in (output / "cleaned" / "mvp" / season / "matches.jsonl")
            .read_text()
            .splitlines()
        ]
        data = {}
        cached_count = 0
        for source in load_sources(season):
            if source.id == "matches":
                continue
            base = output / "raw" / "mvp" / season
            cache = base / ("archive" if source.id.startswith("gw_") else "") / f"{source.id}.csv"
            content, cached = download_source(source, cache, refresh=refresh)
            cached_count += cached
            data[source.id] = source_rows(content, source)
        archive_matches = [
            row for key, rows in data.items() if key.endswith("_matches") for row in rows
        ]
        lineups = [row for key, rows in data.items() if key.endswith("_lineups") for row in rows]
        appearances = [
            row for key, rows in data.items() if key.endswith("_appearances") for row in rows
        ]
        roster, records, coverage = build_player_corpus(
            data["teams"],
            data["players"],
            data["player_totals"],
            archive_matches,
            lineups,
            appearances,
            canonical,
            season,
        )
        report.update(coverage, cached_sources=cached_count, sources=len(data))
        if not report["valid"]:
            raise ValueError("Player evidence coverage gate failed; see report")
        report["normalized_sha256"] = {}
        for name, rows in [("players", roster), ("appearances", records)]:
            payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
            report["normalized_sha256"][name] = hashlib.sha256(payload.encode()).hexdigest()
            atomic_write(output / "cleaned" / "mvp" / season / f"{name}.jsonl", payload.encode())
        report["status"] = "complete"
    except (ValueError, KeyError, OSError, requests.RequestException) as exc:
        report.update(valid=False, status="failed", error=str(exc))
        atomic_write(report_path, (json.dumps(report, indent=2) + "\n").encode())
        raise
    atomic_write(report_path, (json.dumps(report, indent=2) + "\n").encode())
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    report = collect_players(args.season, args.output, refresh=args.refresh)
    print(
        f"Validated {report['players']} players and "
        f"{report['appearance_records']} player-match records"
    )
    print(f"Team fixture coverage: {report['team_fixture_coverage']}")


if __name__ == "__main__":
    main()
