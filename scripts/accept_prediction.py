"""Check live historical predictions against independently rebuilt offline forecasts."""

import argparse
import json
import math
import sys
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.corpus.statistical_text import encoded
from src.ingest.source_download import atomic_write
from src.prediction.service import REPLAY_SEASONS
from src.prediction.sources import load_results
from src.prediction.workflow_evaluation import evaluate_all


def accept(output, url):
    path = output / "reports/prediction/integration-v1.json"
    atomic_write(path, encoded({"valid": False, "status": "incomplete"}))
    rows, sources = load_results(output)
    reference = evaluate_all(rows, sources)

    def request(route, body=None):
        req = Request(
            url.rstrip("/") + route,
            data=encoded(body) if body else None,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=120) as response:
            return json.load(response)

    catalogue = request("/api/prediction/status")
    expected_ids = {m["id"] for m in rows if m["season"] in REPLAY_SEASONS}
    actual_ids = {m["id"] for m in catalogue["fixtures"]}
    if expected_ids != actual_ids or len(catalogue["fixtures"]) != len(expected_ids):
        raise ValueError("Live fixture catalogue differs from verified originals")
    checks = []
    for season in REPLAY_SEASONS:
        fixture = next(
            m
            for m in rows
            if m["season"] == season
            and m["home_team"] == "Liverpool"
            and m["away_team"] == "Bournemouth"
        )
        expected = next(
            m for m in reference["seasons"][season]["predictions"] if m["match_id"] == fixture["id"]
        )
        result = request("/api/analyst/prediction", {"match_id": fixture["id"]})
        if result["fixture"] != {
            key: fixture[key] for key in ("id", "season", "date", "home_team", "away_team")
        }:
            raise ValueError("Live fixture identity differs from request")
        for field in ("probabilities", "expected_goals"):
            if any(
                not math.isclose(result[field][key], value, abs_tol=1e-12, rel_tol=0)
                for key, value in expected[field].items()
            ):
                raise ValueError("Live forecast differs from frozen offline evaluation")
        ids = {
            identifier for group in expected["team_history_ids"].values() for identifier in group
        }
        evidence = result["evidence"]
        if (
            {m["id"] for m in evidence} != ids
            or len(evidence) != len(ids)
            or any(m["date"] >= fixture["date"] for m in evidence)
            or result["feature_cutoff_exclusive"] != fixture["date"]
            or result["model_version"] != reference["model_version"]
            or "home_score" in result["fixture"]
            or "away_score" in result["fixture"]
        ):
            raise ValueError("Live explanation violates its identity/date contract")
        by_id = {m["id"]: m for m in rows}
        for item in evidence:
            if set(item) != {
                "id",
                "date",
                "home_team",
                "away_team",
                "home_score",
                "away_score",
                "source",
            }:
                raise ValueError("Missing explanatory result or provenance fields")
            if any(value != by_id[item["id"]][key] for key, value in item.items()):
                raise ValueError("Live source row differs from the verified original")
        checks.append(
            {"season": season, "match_id": fixture["id"], "evidence_records": len(evidence)}
        )
    report = {
        "valid": True,
        "schema": "prediction-integration-v1",
        "checks": checks,
        "dataset_sha256": reference["dataset_sha256"],
        "catalogue_fixtures": len(expected_ids),
    }
    atomic_write(path, encoded(report))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--url", default="http://127.0.0.1:8010")
    args = parser.parse_args()
    print(json.dumps(accept(args.output, args.url), indent=2))
