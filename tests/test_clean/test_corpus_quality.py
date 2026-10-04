"""Changed artifacts, dangling joins and untraceable evidence cannot pass quality."""

import hashlib
import json

import pytest

from config.sources import load_sources
from scripts.check_corpus import check_corpus
from src.clean.corpus_quality import load_verified_corpus, validate_corpus


def reference(source_id):
    source = next(source for source in load_sources("2025-26") if source.id == source_id)
    return {
        "id": source.id,
        "url": source.url,
        "sha256": source.sha256,
        "revision": source.revision,
        "row": 2,
    }


def write_artifacts(root, rows):
    base = root / "cleaned/mvp/2025-26"
    base.mkdir(parents=True, exist_ok=True)
    hashes = {}
    for name, records in rows.items():
        payload = "".join(json.dumps(row) + "\n" for row in records).encode()
        (base / f"{name}.jsonl").write_bytes(payload)
        hashes[name] = hashlib.sha256(payload).hexdigest()
    reports = root / "reports/mvp/2025-26"
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "matches.json").write_text(
        json.dumps({"valid": True, "status": "complete", "normalized_sha256": hashes["matches"]})
    )
    (reports / "players.json").write_text(
        json.dumps(
            {
                "valid": True,
                "status": "complete",
                "normalized_sha256": {name: hashes[name] for name in ("players", "appearances")},
            }
        )
    )


@pytest.fixture
def corpus(tmp_path, monkeypatch):
    # Coverage mathematics is tested independently by historical_matches tests.
    monkeypatch.setattr("src.clean.corpus_quality.validate_coverage", lambda rows: {"issues": []})
    match_source = reference("matches")
    rows = {
        "matches": [
            {
                "id": "match",
                "season": "2025-26",
                "date": "2025-09-01",
                "home_team": "Aston Villa",
                "away_team": "Liverpool",
                "home_score": 1,
                "away_score": 0,
                "source_id": "matches",
                "source_url": match_source["url"],
                "source_sha256": match_source["sha256"],
                "source_revision": match_source["revision"],
                "source_row": 2,
            }
        ],
        "players": [
            {
                "id": f"player{i}",
                "season": "2025-26",
                "source": reference("players"),
                "teams_represented": [team],
            }
            for i, team in enumerate(["Aston Villa", "Liverpool"])
        ],
        "appearances": [
            {
                "id": f"appearance{i}",
                "season": "2025-26",
                "player_id": f"player{i}",
                "match_id": "match",
                "date": "2025-09-01",
                "team": team,
                "minutes": 90,
                "goals": 1 - i,
                "source": reference("gw_1_appearances"),
                "lineup_source": reference("gw_1_lineups"),
            }
            for i, team in enumerate(["Aston Villa", "Liverpool"])
        ],
    }
    write_artifacts(tmp_path, rows)
    return tmp_path, rows


def test_traceable_joins_pass_and_verified_loader_matches_artifact(corpus):
    root, rows = corpus
    report = check_corpus("2025-26", root)
    assert report["valid"] and len(report["team_match_checks"]) == 2
    assert load_verified_corpus(root, "2025-26") == rows
    assert (root / "reports/mvp/2025-26/quality.md").exists()


def test_post_validation_change_is_rejected(corpus):
    root, _ = corpus
    check_corpus("2025-26", root)
    path = root / "cleaned/mvp/2025-26/players.jsonl"
    path.write_text(path.read_text() + "\n")
    with pytest.raises(ValueError, match="changed after validation"):
        load_verified_corpus(root, "2025-26")
    assert not validate_corpus(root, "2025-26")["valid"]


@pytest.mark.parametrize(
    "field,value,expected",
    [
        ("player_id", "unknown", "dangling"),
        ("date", "2026-09-01", "date differs"),
        ("team", "Arsenal", "attribution"),
        ("goals", 2, "exceed"),
        ("season", "2026-27", "wrong-season"),
    ],
)
def test_independent_reconciliation_rejects_invalid_records(corpus, field, value, expected):
    root, rows = corpus
    rows["appearances"][0][field] = value
    write_artifacts(root, rows)
    report = validate_corpus(root, "2025-26")
    assert not report["valid"]
    assert any(expected in issue for issue in report["issues"])


def test_unknown_source_is_rejected_even_with_fresh_artifact_hashes(corpus):
    root, rows = corpus
    rows["appearances"][0]["source"]["url"] = "https://example.test/unrelated.csv"
    write_artifacts(root, rows)
    report = validate_corpus(root, "2025-26")
    assert not report["valid"]
    assert any("source reference" in issue for issue in report["issues"])


def test_unattributed_goals_are_disclosed_without_inventing_player_totals(corpus):
    root, rows = corpus
    rows["appearances"][0]["goals"] = 0
    write_artifacts(root, rows)
    report = validate_corpus(root, "2025-26")
    assert report["valid"] and any("unattributed" in warning for warning in report["warnings"])


def test_missing_collection_writes_failed_quality_report(tmp_path):
    with pytest.raises(ValueError, match="quality gate failed"):
        check_corpus("2025-26", tmp_path)
    report = json.loads((tmp_path / "reports/mvp/2025-26/quality.json").read_text())
    assert not report["valid"]
