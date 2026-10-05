"""Statistical documents must reflect raw metrics, fixture identity and source rows."""

import copy
import csv
import io
import json
from dataclasses import asdict

import pytest

from config.sources import SourceSpec
from src.corpus import statistical_text as text
from src.corpus.contracts import sha256
from src.ingest.historical_matches import normalize_matches


@pytest.fixture
def corpus(tmp_path, monkeypatch):
    season = "2025-26"
    sources = []

    def raw(source_id, rows):
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        content = buffer.getvalue().encode()
        source = SourceSpec(
            source_id,
            season,
            f"https://example.org/{source_id}.csv",
            "Test publisher",
            "https://example.org/terms",
            "Attribution required",
            "pinned-revision",
            sha256(content),
            list(rows[0]),
            "Fixture",
        )
        sources.append(source)
        base = tmp_path / "raw/mvp" / season
        if source_id.startswith("gw_"):
            base /= "archive"
        base.mkdir(parents=True, exist_ok=True)
        (base / (source_id + ".csv")).write_bytes(content)
        return {
            "id": source_id,
            "url": source.url,
            "sha256": source.sha256,
            "revision": source.revision,
            "row": 2,
        }, content

    ref, content = raw(
        "matches",
        [
            {
                "Div": "E0",
                "Date": "01/09/2025",
                "HomeTeam": "Aston Villa",
                "AwayTeam": "Liverpool",
                "FTHG": "1",
                "FTAG": "0",
                "FTR": "H",
            }
        ],
    )
    match = normalize_matches(content, season, "matches")[0]
    match.update(
        source_url=ref["url"], source_sha256=ref["sha256"], source_revision=ref["revision"]
    )
    raw(
        "teams",
        [
            {"code": "7", "id": "2", "name": "Aston Villa"},
            {"code": "14", "id": "12", "name": "Liverpool"},
        ],
    )
    player_ref, _ = raw(
        "players",
        [
            {
                "player_id": "37",
                "player_code": "101188",
                "first_name": "Lucas",
                "second_name": "Digne",
            }
        ],
    )
    player = {
        "id": "pl:2025-26:player:101188",
        "name": "Lucas Digne",
        "source": player_ref,
        "fpl_season_totals": {"goals_scored": 999},
    }
    stats_ref, _ = raw(
        "gw_1_appearances",
        [
            {
                "player_id": "37",
                "match_id": "archive-fixture",
                "minutes_played": "90",
                "goals": "0",
                "assists": "",
                "xg": "0.25",
                "xa": "",
            }
        ],
    )
    lineup_ref, _ = raw(
        "gw_1_lineups", [{"match_id": "archive-fixture", "player_id": "37", "team_code": "7"}]
    )
    raw(
        "gw_1_matches",
        [
            {
                "match_id": "archive-fixture",
                "home_team": "7",
                "away_team": "14",
                "gameweek": "1",
                "kickoff_time": "2025-09-01T19:00:00+00:00",
                "tournament": "prem",
            }
        ],
    )
    app = {
        "id": player["id"] + ":" + match["id"],
        "player_id": player["id"],
        "source_player_id": 37,
        "match_id": match["id"],
        "match_source_id": "archive-fixture",
        "team": "Aston Villa",
        "date": match["date"],
        "gameweek": 1,
        "minutes": 90,
        "goals": 0,
        "assists": None,
        "expected_goals": 0.25,
        "expected_assists": None,
        "source": stats_ref,
        "lineup_source": lineup_ref,
    }
    rows = {
        "matches": [match],
        "players": [player],
        "appearances": [app, {**copy.deepcopy(app), "id": "unused", "minutes": 0}],
    }
    monkeypatch.setattr(text, "load_sources", lambda season: sources)
    monkeypatch.setattr(text, "load_verified_corpus", lambda output, season: rows)
    return rows, sources


def test_traceable_positive_minute_documents_and_unknowns(tmp_path, corpus):
    report = text.prepare_text(tmp_path, "2025-26")
    assert report["valid"] and report["documents_by_kind"] == {"match": 1, "appearance": 1}
    assert report["counts"]["source_assets"] == 6
    assert report["counts"]["derived_assets"] == 2
    assets, records = text.load_text_documents(tmp_path, "2025-26")
    app = next(row for row in records if row["kind"] == "appearance")
    assert "assists: unknown" in app["text"] and "expected goals: 0.25" in app["text"]
    assert "999" not in app["text"]
    assert len(app["source_refs"]) == 6
    by_id = {asset.id: asset for asset in assets}
    parent = by_id[app["document"]["asset_id"]]
    assert parent.parent_asset_id == app["source_refs"][0]["asset_id"]
    assert parent.event_date.isoformat() == "2025-09-01" and parent.season == "2025-26"
    assert parent.bytes == len(app["text"].encode())
    assert app["document"]["end"] == len(app["text"])
    # En dash has multiple UTF-8 bytes, but document offsets count characters.
    assert parent.bytes > app["document"]["end"]
    first = text.paths(tmp_path, "2025-26")[1].read_bytes()
    assert text.prepare_text(tmp_path, "2025-26")["valid"]
    assert text.paths(tmp_path, "2025-26")[1].read_bytes() == first


@pytest.mark.parametrize("change", ["metric", "lineup", "player", "fixture", "reference", "row"])
def test_false_citations_or_attribution_rejected(tmp_path, corpus, change):
    rows, _ = corpus
    app = rows["appearances"][0]
    if change == "metric":
        app["goals"] = 5
    elif change == "lineup":
        app["team"] = "Liverpool"
    elif change == "player":
        rows["players"][0]["name"] = "Wrong player"
    elif change == "fixture":
        app["date"] = "2025-09-02"
    elif change == "reference":
        app["source"]["revision"] = "changed"
    else:
        app["source"]["row"] = 999
    assert not text.prepare_text(tmp_path, "2025-26")["valid"]


def test_raw_mutation_invalidates_prepared_text(tmp_path, corpus):
    assert text.prepare_text(tmp_path, "2025-26")["valid"]
    path = tmp_path / "raw/mvp/2025-26/archive/gw_1_appearances.csv"
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="checksum"):
        text.load_text_documents(tmp_path, "2025-26")
    assert not text.prepare_text(tmp_path, "2025-26")["valid"]


def test_changed_manifest_rejected_even_with_rewritten_report_hash(tmp_path, corpus):
    text.prepare_text(tmp_path, "2025-26")
    _, docs_path, report_path = text.paths(tmp_path, "2025-26")
    records = [json.loads(line) for line in docs_path.read_text().splitlines()]
    records[0]["source_refs"][0]["row"] = 200
    text.write_lines(docs_path, records)
    with pytest.raises(ValueError, match="manifest changed"):
        text.load_text_documents(tmp_path, "2025-26")
    report = json.loads(report_path.read_text())
    report["documents_sha256"] = sha256(docs_path.read_bytes())
    report_path.write_bytes(text.encoded(report))
    with pytest.raises(ValueError, match="source evidence"):
        text.load_text_documents(tmp_path, "2025-26")


@pytest.mark.parametrize("limits", [{"document_budget": 1}, {"byte_budget": 1}])
def test_budget_failure_withdraws_accepted_manifest(tmp_path, corpus, limits):
    text.prepare_text(tmp_path, "2025-26")
    assert not text.prepare_text(tmp_path, "2025-26", **limits)["valid"]
    with pytest.raises(ValueError, match="incomplete"):
        text.load_text_documents(tmp_path, "2025-26")
    assert not text.paths(tmp_path, "2025-26")[1].exists()


def test_contract_reuse_terms_preserved_without_inventing_license(tmp_path, corpus):
    assets, _, profile = text.build_documents(tmp_path, "2025-26")
    assert all(asset.license_name == "Attribution required" for asset in assets)
    assert profile["sources"] == [
        asdict(source) for source in sorted(corpus[1], key=lambda x: x.id)
    ]
