"""Verify committed independent reference answers against a populated MVP corpus."""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.sources import load_sources
from src.analysis.deductions import analyze, answer
from src.clean.corpus_quality import load_verified_corpus
from src.ingest.source_download import atomic_write

DEFAULT_REFERENCES = (
    Path(__file__).resolve().parent.parent / "tests/fixtures/mvp_reference_questions.json"
)


def verify_references(output: Path, season: str, references: Path = DEFAULT_REFERENCES) -> dict:
    reference = json.loads(references.read_text())
    corpus = load_verified_corpus(output, season)
    source_contracts = load_sources(season)
    contracts = {source.id: source.sha256 for source in source_contracts}
    source_pairs = {(source.url, source.sha256) for source in source_contracts}
    if reference["season"] != season or any(
        contracts.get(key) != digest for key, digest in reference["source_sha256"].items()
    ):
        raise ValueError("Reference questions do not match the configured source contracts")
    checks = []
    for case in reference["cases"]:
        result = analyze(output, season, **case["request"])
        value = result
        for key in case["path"]:
            value = value[key]
        if case.get("projection"):
            value = [{key: row[key] for key in case["projection"]} for row in value]
        matched = value == case["expected"]
        records = {row["id"] for rows in corpus.values() for row in rows}
        traceable = bool(result["sources"]) and all(
            (source["url"], source["source_sha256"]) in source_pairs
            and all(ident in records for ident in source["record_ids"])
            for source in result["sources"]
        )
        if "source_rows" in case:
            actual = sorted(row for source in result["sources"] for row in source["source_rows"])
            traceable = traceable and actual == case["source_rows"]
        checks.append(
            {
                "question": case["question"],
                "passed": matched and traceable,
                "actual": value,
                "expected": case["expected"],
                "traceable": traceable,
            }
        )
    unsupported = [
        "Liverpool points 2024-25",
        "Compare Ramsey and Jones",
        "Liverpool expected goals when xG is missing",
        "What do podcasts say?",
        "Which Liverpool players score most per 90 with at least 9999 minutes?",
    ]
    for question in unsupported:
        result = answer(output, season, question)
        checks.append(
            {
                "question": question,
                "passed": result["status"] == "insufficient_evidence",
                "status": result["status"],
            }
        )
    quality = json.loads((output / "reports/mvp" / season / "quality.json").read_text())
    report = {
        "valid": all(check["passed"] for check in checks),
        "season": season,
        "verified_at": datetime.now(UTC).isoformat(),
        "reference_method": reference["method"],
        "reference_questions": len(reference["cases"]),
        "checks": checks,
        "counts": quality["counts"],
        "artifact_sha256": quality["artifact_sha256"],
        "quality_warnings": quality["warnings"],
    }
    atomic_write(
        output / "reports/mvp" / season / "acceptance.json",
        (json.dumps(report, indent=2, ensure_ascii=False) + "\n").encode(),
    )
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data"))
    parser.add_argument("--season", default="2025-26")
    parser.add_argument("--references", type=Path, default=DEFAULT_REFERENCES)
    args = parser.parse_args()
    report = verify_references(args.output, args.season, args.references)
    print(
        f"Reference acceptance: {sum(row['passed'] for row in report['checks'])}"
        f"/{len(report['checks'])}"
    )
    if not report["valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
