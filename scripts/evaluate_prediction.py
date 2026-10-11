"""Evaluate frozen result-date baselines on verified cached historical PL sources."""

import argparse
import json
import sys
from pathlib import Path

from filelock import FileLock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.corpus.statistical_text import encoded
from src.ingest.match_history import build_history, load_history_sources
from src.ingest.source_download import atomic_write
from src.prediction.evaluation import evaluate


def run(output):
    folder = output / "reports/prediction"
    folder.mkdir(parents=True, exist_ok=True)
    with FileLock(str(folder / "evaluation.lock")):
        path = folder / "historical-hda-v1.json"
        # Withdraw previous success before verifying inputs or recomputing.
        atomic_write(path, encoded({"valid": False, "status": "incomplete"}))
        report = evaluate_sources(output)
        atomic_write(path, encoded(report))
        return report, path


def evaluate_sources(output):
    sources = load_history_sources()
    # Reconstruct from hash-checked original CSVs, never trust derived snapshots.
    _, _, seasons, _ = build_history(output, sources)
    report = evaluate([match for rows in seasons.values() for match in rows])
    report["sources"] = [
        {
            "season": s.season,
            "url": s.url,
            "sha256": s.sha256,
            "revision": s.revision,
            "publisher": s.publisher,
        }
        for s in sources
    ]
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    args = parser.parse_args()
    report, path = run(args.output)
    print(
        json.dumps(
            {
                "selected_window": report["selected_window"],
                "test_metrics": report["test_metrics"],
                "report": str(path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
