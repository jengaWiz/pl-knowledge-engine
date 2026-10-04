"""Independent coverage and evidence regressions for historical match collection."""

import json
from unittest.mock import patch

import pytest

from scripts.collect_matches import collect_matches
from src.ingest.historical_matches import MATCH_STATS, normalize_matches, validate_coverage

HEADER = "Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,HS,AS,HST,AST,HC\n"
ROW = "E0,15/08/2025,Man City,Liverpool,2,1,H,10,8,4,3,\n"


def test_real_fields_aliases_nulls_and_stable_identifiers():
    record = normalize_matches((HEADER + ROW).encode(), "2025-26", "matches")[0]
    assert record["home_team"] == "Manchester City"
    assert record["source_home_team"] == "Man City"
    assert record["source_row"] == 2
    assert record["date"] == "2025-08-15"
    assert record["home_score"] == 2
    assert record["home_corners"] is None
    assert record["away_yellow_cards"] is None
    assert record["id"] == "pl:2025-26:manchester-city:liverpool"
    rescheduled = ROW.replace("15/08/2025", "16/08/2025")
    assert (
        normalize_matches((HEADER + rescheduled).encode(), "2025-26", "matches")[0]["id"]
        == record["id"]
    )


@pytest.mark.parametrize(
    "row",
    [
        ROW.replace("2025", "2026"),
        ROW.replace(",2,1,H,", ",2,1,D,"),
        ROW.replace(",2,1,H,", ",,1,H,"),
        ROW.replace(",2,1,H,", ",-1,1,H,"),
        ROW.replace(",10,8,4,3,", ",2,8,4,3,"),
        ROW.replace("E0,", "E1,"),
        ROW.replace("Man City,Liverpool", "Liverpool,Liverpool"),
    ],
)
def test_invalid_evidence_is_rejected(row):
    with pytest.raises(ValueError):
        normalize_matches((HEADER + row).encode(), "2025-26", "matches")


def complete_schedule():
    teams = ["Aston Villa", "Liverpool"] + [f"Team {i}" for i in range(18)]
    return [
        {
            "home_team": home,
            "away_team": away,
            "date": "2025-09-01",
            **dict.fromkeys(MATCH_STATS.values(), None),
        }
        for home in teams
        for away in teams
        if home != away
    ]


def test_complete_double_round_robin_is_required():
    records = complete_schedule()
    report = validate_coverage(records)
    assert report["valid"]
    assert report["fixtures"] == 380 and report["teams"] == 20
    assert set(report["fixtures_by_team"].values()) == {38}
    assert report["missing_statistics"]["home_shots"] == 380


def test_duplicate_cannot_disguise_missing_fixture():
    records = complete_schedule()
    records[-1] = records[0]
    report = validate_coverage(records)
    assert not report["valid"]
    assert any("Duplicate" in issue for issue in report["issues"])
    assert any("Missing home/away" in issue for issue in report["issues"])


def test_incomplete_run_writes_failure_report_without_publishing_corpus(tmp_path):
    with patch(
        "scripts.collect_matches.download_source", return_value=((HEADER + ROW).encode(), False)
    ):
        with pytest.raises(ValueError, match="completeness gate"):
            collect_matches("2025-26", tmp_path)
    report = json.loads((tmp_path / "reports/mvp/2025-26/matches.json").read_text())
    assert report["status"] == "failed" and not report["valid"]
    assert report["fixtures"] == 1
    assert not (tmp_path / "cleaned/mvp/2025-26/matches.jsonl").exists()


def test_short_csv_row_is_reported_as_invalid_evidence():
    with pytest.raises(ValueError, match="malformed CSV"):
        normalize_matches((HEADER + "E0,15/08/2025,Liverpool\n").encode(), "2025-26", "matches")


def test_successful_collection_publishes_traceable_complete_records(tmp_path):
    teams = ["Aston Villa", "Liverpool"] + [f"Team {i}" for i in range(18)]
    csv_data = "Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR\n" + "".join(
        f"E0,15/08/2025,{home},{away},1,0,H\n" for home in teams for away in teams if home != away
    )
    with patch("scripts.collect_matches.download_source", return_value=(csv_data.encode(), True)):
        report = collect_matches("2025-26", tmp_path)
    rows = [
        json.loads(line)
        for line in (tmp_path / "cleaned/mvp/2025-26/matches.jsonl").read_text().splitlines()
    ]
    assert report["valid"] and report["cached"]
    assert len(rows) == len({row["id"] for row in rows}) == 380
    assert {row["source_row"] for row in rows} == set(range(2, 382))
    assert all(row["source_url"].endswith("2526/E0.csv") for row in rows)
    assert all(row["home_shots"] is None for row in rows)
