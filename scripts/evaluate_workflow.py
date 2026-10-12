"""Evaluate the frozen prediction model on two later seasons, without provider calls."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from filelock import FileLock

from config.settings import settings
from src.corpus.statistical_text import encoded
from src.ingest.source_download import atomic_write
from src.prediction.sources import load_results
from src.prediction.workflow_evaluation import evaluate_all


def run(output):
    folder = output / "reports/prediction"
    folder.mkdir(parents=True, exist_ok=True)
    with FileLock(str(folder / "workflow.lock")):
        path = folder / "workflow-v1.json"
        atomic_write(path, encoded({"valid": False, "status": "incomplete"}))
        report = evaluate_all(*load_results(output))
        atomic_write(path, encoded(report))
        return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    result = run(parser.parse_args().output)
    for season, report in result["seasons"].items():
        print(season, {key: report[key] for key in ("rolling", "league", "uncertainty")})
