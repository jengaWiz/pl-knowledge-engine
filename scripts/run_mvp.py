"""Run the no-key historical MVP collection pipeline with durable stage status."""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from filelock import FileLock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from scripts.check_corpus import check_corpus
from scripts.collect_commentary import collect_commentary
from scripts.collect_matches import collect_matches
from scripts.collect_players import collect_players
from src.ingest.source_download import atomic_write


def run_mvp(
    season: str, output: Path, *, refresh: bool = False, commentary: bool = False, stages=None
) -> dict:
    """Resume verified source caches, recheck derived artifacts, and fail fast."""
    operations = (
        stages
        if stages is not None
        else {
            "matches": lambda: collect_matches(season, output, refresh=refresh),
            "players": lambda: collect_players(season, output, refresh=refresh),
            "quality": lambda: check_corpus(season, output),
        }
    )
    if commentary and stages is None:
        operations["commentary"] = lambda: collect_commentary(season, output)
    target = output / "reports" / "mvp" / season
    target.mkdir(parents=True, exist_ok=True)
    report = {
        "season": season,
        "started_at": datetime.now(UTC).isoformat(),
        "status": "running",
        "stages": {},
    }
    with FileLock(str(target / "pipeline.lock"), timeout=0):
        for name, operation in operations.items():
            report["current_stage"] = name
            report["stages"][name] = {"status": "running"}
            atomic_write(target / "pipeline.json", (json.dumps(report, indent=2) + "\n").encode())
            try:
                result = operation()
                if result.get("valid") is False:
                    raise ValueError(f"{name} returned a failed gate")
                report["stages"][name] = {"status": "complete", "result": result}
            except Exception as exc:
                report.update(status="failed", finished_at=datetime.now(UTC).isoformat())
                report["stages"][name] = {"status": "failed", "error": str(exc)}
                atomic_write(
                    target / "pipeline.json", (json.dumps(report, indent=2) + "\n").encode()
                )
                raise
        report.update(status="complete", finished_at=datetime.now(UTC).isoformat())
        report.pop("current_stage", None)
        atomic_write(target / "pipeline.json", (json.dumps(report, indent=2) + "\n").encode())
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--commentary", action="store_true", help="Try bounded public metadata")
    args = parser.parse_args()
    report = run_mvp(args.season, args.output, refresh=args.refresh, commentary=args.commentary)
    print(f"MVP pipeline {report['status']}: {', '.join(report['stages'])}")


if __name__ == "__main__":
    main()
