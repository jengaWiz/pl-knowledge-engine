import csv
import io
import json

import pytest

from config.sources import SourceSpec
from scripts import evaluate_workflow
from src.corpus.contracts import sha256
from src.prediction import sources


@pytest.fixture
def originals(tmp_path, monkeypatch):
    specs = []
    teams = ["Aston Villa", "Liverpool", *[f"Club {i}" for i in range(18)]]
    fields = ["Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]
    paths = []
    for year in range(2022, 2026):
        season = f"{year}-{(year + 1) % 100:02d}"
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(fields)
        for home in teams:
            for away in teams:
                if home != away:
                    writer.writerow(["E0", f"01/09/{year}", home, away, 1, 0, "H"])
        content = buffer.getvalue().encode()
        digest = sha256(content)
        specs.append(
            SourceSpec(
                "matches",
                season,
                "https://example.org/results.csv",
                "Publisher",
                "https://example.org/terms",
                "Attribution",
                "sha256:" + digest,
                digest,
                fields,
                "Fixture",
            )
        )
        path = tmp_path / ("raw/mvp" if year == 2025 else "raw/history") / season / "matches.csv"
        path.parent.mkdir(parents=True)
        path.write_bytes(content)
        paths.append(path)
    monkeypatch.setattr(sources, "load_history_sources", lambda: specs[:3])
    monkeypatch.setattr(sources, "load_sources", lambda season: specs[3:])
    return tmp_path, paths


def test_all_originals_reparsed_with_exact_row_references(originals):
    output, _ = originals
    rows, refs = sources.load_results(output)
    assert len(rows) == 1520 and len(refs) == 4
    assert {row["season"] for row in rows} == {"2022-23", "2023-24", "2024-25", "2025-26"}
    assert all(row["source"]["row"] == row["source_row"] for row in rows)
    assert all(row["source"]["revision"] == "sha256:" + row["source"]["sha256"] for row in rows)


@pytest.mark.parametrize("index", [0, 3])
def test_changed_historical_or_primary_csv_rejected(originals, index):
    output, paths = originals
    paths[index].write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksum"):
        sources.load_results(output)


def test_workflow_failure_withdraws_stale_report(tmp_path, monkeypatch):
    path = tmp_path / "reports/prediction/workflow-v1.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"valid": True, "old": True}))

    def unavailable(output):
        raise FileNotFoundError("missing source")

    monkeypatch.setattr(evaluate_workflow, "load_results", unavailable)
    with pytest.raises(FileNotFoundError):
        evaluate_workflow.run(tmp_path)
    assert json.loads(path.read_text()) == {"valid": False, "status": "incomplete"}
