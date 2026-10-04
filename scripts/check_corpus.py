"""Verify normalized MVP evidence before loading or making deductions."""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.clean.corpus_quality import validate_corpus
from src.ingest.source_download import atomic_write


def check_corpus(season: str, output: Path) -> dict:
    report = {"checked_at": datetime.now(UTC).isoformat(), "season": season}
    target = output / "reports" / "mvp" / season
    try:
        report.update(validate_corpus(output, season))
    except (ValueError, KeyError, OSError) as exc:
        report.update(valid=False, issues=[str(exc)])
    atomic_write(target / "quality.json", (json.dumps(report, indent=2) + "\n").encode())
    markdown = f"# MVP corpus quality — {season}\n\nPassed: {report['valid']}\n\n"
    markdown += f"Counts: {report.get('counts', {})}\n\n## Issues\n\n"
    markdown += "\n".join("- " + issue for issue in report.get("issues", [])) or "None."
    markdown += "\n\n## Warnings and limitations\n\n"
    markdown += "\n".join("- " + warning for warning in report.get("warnings", [])) or "None."
    atomic_write(target / "quality.md", (markdown + "\n").encode())
    if not report["valid"]:
        raise ValueError("Corpus quality gate failed; see quality.json")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    args = parser.parse_args()
    report = check_corpus(args.season, args.output)
    print(
        f"Dataset quality gate passed: {report['counts']}; "
        f"{len(report['warnings'])} disclosed warnings"
    )


if __name__ == "__main__":
    main()
