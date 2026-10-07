"""Historical seasons are complete, separated, pinned and reproducible from raw rows."""

import csv
import io
import json
from dataclasses import replace

import pytest
import requests

from config.sources import SourceSpec
from src.corpus.contracts import sha256
from src.ingest import match_history as history


@pytest.fixture
def season(tmp_path, monkeypatch):
    names = ["Aston Villa", "Liverpool", *[f"Club {n}" for n in range(18)]]
    buffer = io.StringIO()
    fields = ["Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for home in names:
        for away in names:
            if home != away:
                writer.writerow(
                    dict(zip(fields, ["E0", "01/09/2024", home, away, "1", "0", "H"], strict=True))
                )
    content = buffer.getvalue().encode()
    source = SourceSpec(
        "matches",
        "2024-25",
        "https://www.football-data.co.uk/mmz4281/2425/E0.csv",
        "Publisher",
        "https://www.football-data.co.uk/englandm.php",
        "Retain attribution",
        "sha256:" + sha256(content),
        sha256(content),
        fields,
        "Test",
    )
    sources = [source]
    monkeypatch.setattr(history, "load_history_sources", lambda: sources)
    path = history.raw_path(tmp_path, source.season)
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    return source, sources, path


def test_complete_season_repeat_provenance_and_unknowns(tmp_path, season):
    source, _, _ = season
    first = history.collect_history(tmp_path)
    assert first["valid"] and first["coverage"]["2024-25"]["fixtures"] == 380
    assert first["counts"]["source_assets"] == 1 and first["counts"]["derived_assets"] == 380
    assert first["cached_seasons"] == ["2024-25"]
    assets, records, summary = history.load_history_documents(tmp_path)
    assert len(records) == 380 and len(assets) == 381
    assert all(row["season"] == "2024-25" for row in records)
    assert all("Shots: unknown–unknown" in row["text"] for row in records)
    assert records[0]["source_refs"][0]["sha256"] == source.sha256
    assert all(2 <= row["source_refs"][0]["row"] <= 381 for row in records)
    manifest = history.paths(tmp_path)[1].read_bytes()
    assert history.collect_history(tmp_path)["valid"]
    assert history.paths(tmp_path)[1].read_bytes() == manifest


def test_corrupt_raw_cache_fails_without_redownload(tmp_path, season, monkeypatch):
    history.collect_history(tmp_path)
    season[2].write_bytes(b"corrupt")
    monkeypatch.setattr(
        requests.Session, "get", lambda *args, **kwargs: pytest.fail("Unexpected network")
    )
    with pytest.raises(ValueError, match="checksum"):
        history.load_history_documents(tmp_path)
    assert not history.collect_history(tmp_path)["valid"]
    assert not history.paths(tmp_path)[1].exists()


@pytest.mark.parametrize("limits", [{"document_budget": 379}, {"byte_budget": 1}])
def test_budget_failure_withdraws_previous_manifest(tmp_path, season, limits):
    history.collect_history(tmp_path)
    assert not history.collect_history(tmp_path, **limits)["valid"]
    with pytest.raises(ValueError, match="incomplete"):
        history.load_history_documents(tmp_path)


def test_normalized_tampering_rejected_even_with_changed_report_hash(tmp_path, season):
    history.collect_history(tmp_path)
    path = tmp_path / "cleaned/history/2024-25/matches.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    rows[0]["home_score"] = 99
    history.write_lines(path, rows)
    report_path = history.paths(tmp_path)[2]
    report = json.loads(report_path.read_text())
    report["normalized_sha256"]["2024-25"] = sha256(path.read_bytes())
    report_path.write_bytes(history.encoded(report))
    with pytest.raises(ValueError, match="raw source"):
        history.load_history_documents(tmp_path)


def test_document_manifest_tampering_rejected(tmp_path, season):
    history.collect_history(tmp_path)
    path = history.paths(tmp_path)[1]
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="manifest changed"):
        history.load_history_documents(tmp_path)


@pytest.mark.parametrize("change", ["missing_fixture", "wrong_season"])
def test_bad_reviewed_content_still_fails_coverage_or_dates(tmp_path, season, change):
    source, sources, path = season
    content = path.read_bytes()
    if change == "missing_fixture":
        content = b"\n".join(content.splitlines()[:-1]) + b"\n"
    else:
        content = content.replace(b"01/09/2024", b"01/09/2023")
    path.write_bytes(content)
    sources[0] = replace(source, sha256=sha256(content), revision="sha256:" + sha256(content))
    report = history.collect_history(tmp_path)
    assert not report["valid"]
    assert "completeness" in report["detail"] or "season" in report["detail"]


def test_new_source_configuration_invalidates_previous_acceptance(tmp_path, season):
    history.collect_history(tmp_path)
    season[1][0] = replace(season[0], reuse_terms="New terms require review")
    with pytest.raises(ValueError, match="source evidence"):
        history.load_history_documents(tmp_path)


def test_interrupted_download_keeps_verified_cache_but_invalidates_outputs(
    tmp_path, season, monkeypatch
):
    history.collect_history(tmp_path)

    def fail(*args, **kwargs):
        raise requests.Timeout("Network interrupted")

    monkeypatch.setattr(history, "download_source", fail)
    assert not history.collect_history(tmp_path)["valid"]
    assert season[2].exists() and not history.paths(tmp_path)[1].exists()


def test_packaged_source_inventory_is_season_specific():
    sources = history.load_history_sources()
    assert [source.season for source in sources] == ["2022-23", "2023-24", "2024-25"]
    assert all(
        source.id == "matches" and source.revision == "sha256:" + source.sha256
        for source in sources
    )
