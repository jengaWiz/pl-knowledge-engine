"""Prepare and verify optional evidence in the current API's persistent data directory."""

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from filelock import FileLock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.ingest.source_download import atomic_write

STAGES = (
    ("collect_history.py", ()),
    ("prepare_text.py", ()),
    ("accept_corpus.py", ("--require-target",)),
    ("index_evidence.py", ()),
    ("load_evidence_graph.py", ()),
    ("accept_retrieval.py", ()),
)


def prepare(output, season, *, execute=None):
    folder = output / "reports/evidence" / season
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "pipeline.json"

    def save():
        atomic_write(path, (json.dumps(report, indent=2) + "\n").encode())

    def run(script, flags):
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).parent / script),
                "--output",
                str(output),
                *flags,
                *([] if script == "collect_history.py" else ["--season", season]),
            ],
            check=True,
        )

    with FileLock(str(folder / "pipeline.lock"), timeout=0):
        report = {
            "schema": "evidence-preparation-v1",
            "status": "running",
            "season": season,
            "started_at": datetime.now(UTC).isoformat(),
            "stages": {},
        }
        save()
        try:
            for script, flags in STAGES:
                report["current_stage"] = script
                report["stages"][script] = "running"
                save()
                print(f"Preparing evidence: {script}", flush=True)
                (execute or run)(script, flags)
                report["stages"][script] = "complete"
                save()
            report.update(status="complete", finished_at=datetime.now(UTC).isoformat())
            report.pop("current_stage")
        except Exception as exc:
            report.update(
                status="failed", error=type(exc).__name__, finished_at=datetime.now(UTC).isoformat()
            )
            save()
            raise
        save()
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--season", default=settings.season)
    args = parser.parse_args()
    settings.require_credentials("neo4j_password")
    prepare(args.output, args.season)
    print("Expanded evidence preparation and retrieval acceptance complete", flush=True)


if __name__ == "__main__":
    main()
