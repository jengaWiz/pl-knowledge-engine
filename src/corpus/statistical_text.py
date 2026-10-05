"""Render useful season evidence while retaining every contributing CSV reference."""

import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from filelock import FileLock

from config.sources import inspect_source, load_sources
from src.clean.corpus_quality import load_verified_corpus
from src.corpus.contracts import Asset, Document, content_id, count_corpus, document_id, sha256
from src.ingest.historical_matches import TEAM_NAMES, normalize_matches
from src.ingest.historical_players import number, source_rows
from src.ingest.source_download import atomic_write

SCHEMA = "statistical-text-v1"


def encoded(value) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False) + "\n").encode()


def build_documents(output: Path, season: str) -> tuple[list[Asset], list[dict], dict]:
    """Recheck pinned files and row references; do not interpret snapshots as matches."""
    corpus = load_verified_corpus(output, season)
    contracts = {source.id: source for source in load_sources(season)}
    originals, csv_rows = {}, {}

    def ensure_source(source_id):
        source = contracts[source_id]
        if source.id not in csv_rows:
            path = output / "raw/mvp" / season
            if source.id.startswith("gw_"):
                path /= "archive"
            content = (path / (source.id + ".csv")).read_bytes()
            inspect_source(source, content)
            csv_rows[source.id] = {
                row["_source"]["row"]: row for row in source_rows(content, source)
            }
            asset = Asset(
                id=content_id("asset", source.sha256),
                sha256=source.sha256,
                modality="text",
                mime_type="text/csv",
                bytes=len(content),
                source_url=source.url,
                reference_url=source.reference_url,
                publisher=source.publisher,
                attribution=source.publisher,
                license_name=source.reuse_terms,
                license_url=source.reference_url,
                scope="background",
            )
            originals[source.id] = asset

    def source_reference(reference):
        source = contracts[reference["id"]]
        if any(reference[key] != getattr(source, key) for key in ("url", "sha256", "revision")):
            raise ValueError("Statistical source reference differs from its pinned contract")
        ensure_source(source.id)
        number = reference["row"]
        if type(number) is not int or number not in csv_rows[source.id]:
            raise ValueError("Statistical source row is absent")
        return {**reference, "asset_id": originals[source.id].id}

    def match_reference(match):
        return {
            key: match["source_" + field]
            for key, field in (
                ("id", "id"),
                ("url", "url"),
                ("sha256", "sha256"),
                ("revision", "revision"),
                ("row", "row"),
            )
        }

    def known(value):
        return "unknown" if value is None else str(value)

    pending = []
    matches = {row["id"]: row for row in corpus["matches"]}
    players = {row["id"]: row for row in corpus["players"]}
    ensure_source("matches")
    match_source = contracts["matches"]
    raw_matches = (output / "raw/mvp" / season / "matches.csv").read_bytes()
    normalized = {row["id"]: row for row in normalize_matches(raw_matches, season, "matches")}
    for match in corpus["matches"]:
        expected = normalized[match["id"]]
        if any(match[key] != value for key, value in expected.items()):
            raise ValueError("Match statistics disagree with their raw source row")
        if match_reference(match) != {
            "id": match_source.id,
            "url": match_source.url,
            "sha256": match_source.sha256,
            "revision": match_source.revision,
            "row": expected["source_row"],
        }:
            raise ValueError("Match source row mapping changed")
        text = (
            f"{season} Premier League, {match['date']}: {match['home_team']} "
            f"{match['home_score']}–{match['away_score']} {match['away_team']}. "
            f"Shots: {known(match['home_shots'])}–{known(match['away_shots'])}; "
            f"shots on target: {known(match['home_shots_on_target'])}–"
            f"{known(match['away_shots_on_target'])}."
        )
        pending.append((text, "match", match["date"], [match["id"]], [match_reference(match)]))
    for app in corpus["appearances"]:
        if app["minutes"] <= 0:
            continue
        match, player = matches[app["match_id"]], players[app["player_id"]]
        if app["date"] != match["date"] or app["team"] not in (
            match["home_team"],
            match["away_team"],
        ):
            raise ValueError("Appearance has invalid date or match-team attribution")
        text = (
            f"{season} Premier League, {app['date']}: {player['name']} represented "
            f"{app['team']} in {match['home_team']} {match['home_score']}–"
            f"{match['away_score']} {match['away_team']}. "
            f"Minutes: {app['minutes']}; goals: {known(app['goals'])}; "
            f"assists: {known(app['assists'])}; expected goals: {known(app['expected_goals'])}; "
            f"expected assists: {known(app['expected_assists'])}. "
            "Team attribution comes from the match lineup; these are match statistics, "
            "not cumulative FPL snapshots."
        )
        # Team labels also depend on the archived team-code mapping.
        ensure_source("teams")
        team_rows = [
            row
            for row in csv_rows["teams"].values()
            if TEAM_NAMES.get(row["name"], row["name"]) == app["team"]
        ]
        if len(team_rows) != 1:
            raise ValueError("Appearance team has no unique source mapping")
        archive_id = f"gw_{app['gameweek']}_matches"
        ensure_source(archive_id)
        archive_rows = [
            row
            for row in csv_rows[archive_id].values()
            if row["match_id"] == app["match_source_id"]
        ]
        if len(archive_rows) != 1:
            raise ValueError("Appearance fixture has no unique source mapping")
        for ref in (app["source"], app["lineup_source"], player["source"]):
            source_reference(ref)
        stats = csv_rows[app["source"]["id"]][app["source"]["row"]]
        lineup = csv_rows[app["lineup_source"]["id"]][app["lineup_source"]["row"]]
        identity = csv_rows[player["source"]["id"]][player["source"]["row"]]
        for field, raw_field, integer in (
            ("minutes", "minutes_played", True),
            ("goals", "goals", True),
            ("assists", "assists", True),
            ("expected_goals", "xg", False),
            ("expected_assists", "xa", False),
        ):
            if app[field] != number(
                stats.get(raw_field), integer=integer, optional=field != "minutes"
            ):
                raise ValueError("Appearance statistics disagree with their raw source row")
        if (
            player["name"] != f"{identity['first_name']} {identity['second_name']}".strip()
            or player["id"] != f"pl:{season}:player:{identity['player_code']}"
            or any(
                number(row["player_id"], integer=True) != app["source_player_id"]
                for row in (stats, lineup, identity)
            )
            or stats["match_id"] != app["match_source_id"]
            or lineup["match_id"] != app["match_source_id"]
            or number(lineup["team_code"], integer=True)
            != number(team_rows[0]["code"], integer=True)
        ):
            raise ValueError("Appearance identity or lineup attribution disagrees with source")
        archive = archive_rows[0]
        codes = {
            number(row["code"], integer=True): TEAM_NAMES.get(row["name"], row["name"])
            for row in csv_rows["teams"].values()
        }
        if (
            any(
                codes[number(archive[side + "_team"], integer=True)] != match[side + "_team"]
                for side in ("home", "away")
            )
            or datetime.fromisoformat(archive["kickoff_time"]).date().isoformat() != app["date"]
            or number(archive["gameweek"], integer=True) != app["gameweek"]
        ):
            raise ValueError("Appearance fixture mapping disagrees with source")
        refs = [
            app["source"],
            app["lineup_source"],
            player["source"],
            match_reference(match),
            team_rows[0]["_source"],
            archive_rows[0]["_source"],
        ]
        pending.append(
            (text, "appearance", app["date"], [app["id"], player["id"], match["id"]], refs)
        )
    if not pending:
        raise ValueError("No useful statistical evidence")
    # Resolve every reference before finalizing a source-sensitive extraction version.
    resolved = [([source_reference(ref) for ref in item[-1]], item) for item in pending]
    profile = {
        "schema": SCHEMA,
        "season": season,
        "corpus_sha256": sha256(encoded(corpus)),
        "sources": [vars(contracts[key]) for key in sorted(originals)],
    }
    version = SCHEMA + ":" + sha256(encoded(profile))
    derived, records = {}, []
    for refs, (text, kind, date, ids, _) in resolved:
        primary = originals[refs[0]["id"]]
        checksum = sha256(text.encode())
        asset = Asset(
            **{
                **primary.model_dump(),
                "id": content_id("asset", checksum),
                "sha256": checksum,
                "mime_type": "text/plain",
                "bytes": len(text.encode()),
                "parent_asset_id": primary.id,
                "scope": "seasonal",
                "season": season,
                "event_date": date,
                "entity_ids": tuple(ids[1:] if kind == "appearance" else ids),
            }
        )
        derived[asset.id] = asset
        doc = Document(
            id=document_id(asset.id, "text", 0, len(text), version, checksum),
            asset_id=asset.id,
            modality="text",
            sha256=checksum,
            extraction_version=version,
            start=0,
            end=len(text),
            coordinate="characters",
        )
        records.append(
            {
                "document": doc.model_dump(),
                "text": text,
                "kind": kind,
                "season": season,
                "event_date": date,
                "record_ids": ids,
                "source_refs": refs,
            }
        )
    unique_sources = {asset.id: asset for asset in originals.values()}
    return [*unique_sources.values(), *derived.values()], records, profile


def paths(output: Path, season: str):
    base = output / "cleaned/multimodal" / season
    return (
        base / "text_assets.jsonl",
        base / "text_documents.jsonl",
        (output / "reports/multimodal" / season / "text.json"),
    )


def write_lines(path: Path, values):
    atomic_write(path, b"".join(encoded(value) for value in values))


def prepare_text(output: Path, season: str, *, document_budget=5000, byte_budget=10_000_000):
    if document_budget <= 0 or byte_budget <= 0:
        raise ValueError("Text extraction budgets must be positive")
    lock = output / "reports/mvp" / season / "pipeline.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    assets_path, docs_path, report_path = paths(output, season)
    with FileLock(str(lock), timeout=0):
        report = {"valid": False, "schema": SCHEMA, "season": season}
        atomic_write(report_path, encoded(report))
        assets_path.unlink(missing_ok=True)
        docs_path.unlink(missing_ok=True)
        try:
            assets, records, profile = build_documents(output, season)
            payload_bytes = sum(len(row["text"].encode()) for row in records)
            if len(records) > document_budget or payload_bytes > byte_budget:
                raise ValueError("Statistical text exceeds the extraction budget")
            counts = count_corpus(
                assets, [Document.model_validate(row["document"]) for row in records]
            )
            write_lines(assets_path, [asset.model_dump(mode="json") for asset in assets])
            write_lines(docs_path, records)
            report.update(
                valid=True,
                profile=profile,
                counts=counts,
                payload_bytes=payload_bytes,
                documents_by_kind=dict(Counter(row["kind"] for row in records)),
                assets_sha256=sha256(assets_path.read_bytes()),
                documents_sha256=sha256(docs_path.read_bytes()),
            )
        except (ValueError, KeyError, TypeError, OSError) as exc:
            assets_path.unlink(missing_ok=True)
            docs_path.unlink(missing_ok=True)
            report.update(error=type(exc).__name__, detail=str(exc)[:500])
        atomic_write(report_path, encoded(report))
        return report


def load_text_documents(output: Path, season: str) -> tuple[list[Asset], list[dict]]:
    lock = output / "reports/mvp" / season / "pipeline.lock"
    with FileLock(str(lock), timeout=0):
        assets_path, docs_path, report_path = paths(output, season)
        report = json.loads(report_path.read_text())
        if (
            not report.get("valid")
            or report.get("schema") != SCHEMA
            or report.get("season") != season
        ):
            raise ValueError("Statistical text extraction is incomplete")
        if sha256(assets_path.read_bytes()) != report["assets_sha256"] or (
            sha256(docs_path.read_bytes()) != report["documents_sha256"]
        ):
            raise ValueError("Statistical text manifest changed")
        assets, records, profile = build_documents(output, season)
        expected_assets = b"".join(encoded(asset.model_dump(mode="json")) for asset in assets)
        expected_docs = b"".join(encoded(row) for row in records)
        counts = count_corpus(assets, [Document.model_validate(row["document"]) for row in records])
        if (
            assets_path.read_bytes() != expected_assets
            or docs_path.read_bytes() != expected_docs
            or report["profile"] != profile
            or report["counts"] != counts
            or report["payload_bytes"] != sum(len(row["text"].encode()) for row in records)
            or report["documents_by_kind"] != dict(Counter(row["kind"] for row in records))
        ):
            raise ValueError("Statistical text no longer matches its source evidence")
        return assets, records
