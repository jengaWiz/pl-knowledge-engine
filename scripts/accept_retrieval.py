"""Run the frozen local fixture/player/abstention retrieval smoke set."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.corpus.contracts import sha256
from src.corpus.statistical_text import encoded
from src.ingest.source_download import atomic_write
from src.retrieval.hybrid import retrieve

FIXTURES = (
    Path(__file__).resolve().parent.parent / "tests/fixtures/evidence_retrieval_questions.json"
)


def check_result(group, case, result):
    hits, resolved = result["hits"], result["route"]
    if group == "abstentions":
        return resolved["status"] == case["status"] and not hits
    if resolved["status"] != "resolved" or not hits:
        return False
    if any(
        hit["metadata"]["season"] != case["season"] or not hit["graph"]["source_refs"]
        for hit in hits
    ):
        return False
    if group == "fixtures":
        return len(hits) == 1 and all(
            hits[0]["graph"][key] == case[key] for key in ("match_id", "home_score", "away_score")
        )
    return (
        len(hits) == len(case["matches"])
        and {hit["graph"]["match_id"] for hit in hits} == set(case["matches"])
        and all(
            all(hit["graph"][key] == case[key] for key in ("player_id", "team", "opponent"))
            for hit in hits
        )
    )


def accept(output, primary_season, uri, user, password):
    raw = FIXTURES.read_bytes()
    cases = json.loads(raw)
    path = output / "reports/evidence" / primary_season / "hybrid-smoke.json"
    report = {
        "valid": False,
        "schema": cases["schema"],
        "scope": cases["scope"],
        "fixture_sha256": sha256(raw),
        "checks": [],
    }
    atomic_write(path, encoded(report))
    try:
        dataset = None
        for group in ("fixtures", "players", "abstentions"):
            for case in cases[group]:
                result = retrieve(
                    output,
                    primary_season,
                    case["query"],
                    uri,
                    user,
                    password,
                    evidence_season=case["season"],
                    limit=5,
                )
                dataset = dataset or result["graph_dataset_id"]
                if result["graph_dataset_id"] != dataset:
                    raise ValueError("Evidence changed between acceptance cases")
                passed = check_result(group, case, result)
                report["checks"].append(
                    {"group": group, "case": case, "passed": passed, "result": result}
                )
                print(f"{group}: {'PASS' if passed else 'FAIL'} — {case['query']}", flush=True)
        report.update(
            graph_dataset_id=dataset,
            passed=sum(item["passed"] for item in report["checks"]),
            total=len(report["checks"]),
        )
        report["valid"] = report["passed"] == report["total"]
        if not report["valid"]:
            raise ValueError("Retrieval smoke acceptance failed")
    except Exception as exc:
        report.update(error=type(exc).__name__, detail=str(exc)[:500])
        atomic_write(path, encoded(report))
        raise
    atomic_write(path, encoded(report))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--season", default=settings.season)
    args = parser.parse_args()
    settings.require_credentials("neo4j_password")
    report = accept(
        args.output, args.season, settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password
    )
    print(f"Retrieval smoke: {report['passed']}/{report['total']} passed")


if __name__ == "__main__":
    main()
