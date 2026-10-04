"""Reconcile the MVP evidence corpus and prevent loading changed artifacts."""

import hashlib
import json
from collections import defaultdict
from pathlib import Path

from config.sources import load_sources
from src.ingest.historical_matches import validate_coverage
from src.ingest.historical_players import FOCUS_TEAMS


def corpus_paths(output: Path, season: str) -> dict[str, Path]:
    return {
        name: output / "cleaned" / "mvp" / season / f"{name}.jsonl"
        for name in ("matches", "players", "appearances")
    }


def read_corpus(output: Path, season: str) -> tuple[dict, dict]:
    rows, hashes = {}, {}
    for name, path in corpus_paths(output, season).items():
        content = path.read_bytes()
        hashes[name] = hashlib.sha256(content).hexdigest()
        rows[name] = [json.loads(line) for line in content.decode().splitlines() if line]
    return rows, hashes


def validate_corpus(output: Path, season: str) -> dict:
    rows, hashes = read_corpus(output, season)
    reports = {
        name: json.loads((output / "reports" / "mvp" / season / f"{name}.json").read_text())
        for name in ("matches", "players")
    }
    issues, warnings = [], []
    for name, report in reports.items():
        if not report.get("valid") or report.get("status") != "complete":
            issues.append(f"{name} collection report is not complete")
    expected = {
        "matches": reports["matches"].get("normalized_sha256"),
        **reports["players"].get("normalized_sha256", {}),
    }
    for name, checksum in hashes.items():
        if checksum != expected.get(name):
            issues.append(f"{name}: normalized artifact changed or lacks a recorded checksum")
    for name, records in rows.items():
        if len({row["id"] for row in records}) != len(records):
            issues.append(f"{name}: duplicate canonical identifiers")
        if any(row.get("season") != season for row in records):
            issues.append(f"{name}: wrong-season records")
    issues.extend(validate_coverage(rows["matches"])["issues"])
    matches = {row["id"]: row for row in rows["matches"]}
    players = {row["id"]: row for row in rows["players"]}
    contracts = {source.id: source for source in load_sources(season)}

    def check_reference(reference, label):
        source = contracts.get(reference.get("id")) if reference else None
        if source is None or any(
            reference.get(field) != getattr(source, field)
            for field in ("url", "sha256", "revision")
        ):
            issues.append(f"{label}: unknown or changed source reference")
        elif not isinstance(reference.get("row"), int) or reference["row"] < 2:
            issues.append(f"{label}: missing source row")

    for row in rows["matches"]:
        check_reference(
            {
                "id": row.get("source_id"),
                "url": row.get("source_url"),
                "sha256": row.get("source_sha256"),
                "revision": row.get("source_revision"),
                "row": row.get("source_row"),
            },
            row["id"],
        )
    for row in rows["players"]:
        check_reference(row["source"], row["id"])
        if row.get("fpl_source"):
            check_reference(row["fpl_source"], row["id"] + " FPL snapshot")
    grouped = defaultdict(list)
    for row in rows["appearances"]:
        match, player = matches.get(row["match_id"]), players.get(row["player_id"])
        if not match or not player:
            issues.append(f"{row['id']}: dangling player or fixture reference")
            continue
        if (
            row["team"] not in (match["home_team"], match["away_team"])
            or row["team"] not in FOCUS_TEAMS
        ):
            issues.append(f"{row['id']}: invalid match-team attribution")
        if row["date"] != match["date"]:
            issues.append(f"{row['id']}: appearance date differs from fixture")
        if row["team"] not in player["teams_represented"]:
            issues.append(f"{row['id']}: roster does not record the represented team")
        check_reference(row["source"], row["id"])
        check_reference(row["lineup_source"], row["id"] + " lineup")
        grouped[row["match_id"], row["team"]].append(row)
    team_checks = []
    for match in rows["matches"]:
        for side in ("home", "away"):
            team = match[f"{side}_team"]
            if team not in FOCUS_TEAMS:
                continue
            appearances = grouped[match["id"], team]
            goals = sum(row["goals"] for row in appearances if row["goals"] is not None)
            minutes = sum(row["minutes"] for row in appearances)
            if not appearances:
                issues.append(f"{match['id']}: missing {team} player evidence")
            if goals > match[f"{side}_score"]:
                issues.append(f"{match['id']}: player goals exceed the primary team score")
            if goals < match[f"{side}_score"]:
                warnings.append(
                    f"{match['id']}: {team} has goals unattributed to player records; "
                    "own goals or missing archive records are possible explanations"
                )
            team_checks.append(
                {
                    "match_id": match["id"],
                    "team": team,
                    "recorded_player_goals": goals,
                    "team_goals": match[f"{side}_score"],
                    "recorded_player_minutes": minutes,
                }
            )
    if reports["players"].get("fixture_conflicts"):
        warnings.append(
            "Archive score conflict resolved using primary results; see fixture_conflicts"
        )
    if reports["players"].get("lineups_without_stats"):
        warnings.append(
            "Unused/substitute lineup entries without statistics remain unknown, not zero"
        )
    return {
        "valid": not issues,
        "season": season,
        "artifact_sha256": hashes,
        "counts": {name: len(records) for name, records in rows.items()},
        "issues": issues,
        "warnings": warnings,
        "team_match_checks": team_checks,
        "fixture_conflicts": reports["players"].get("fixture_conflicts", []),
        "metric_missingness": reports["players"].get("metric_missingness", {}),
        "source_precedence": (
            "Football-Data results; archive lineups for match-team attribution; "
            "latest FPL snapshots kept separately"
        ),
    }


def load_verified_corpus(output: Path, season: str) -> dict:
    report = json.loads((output / "reports" / "mvp" / season / "quality.json").read_text())
    if not report.get("valid") or report.get("season") != season:
        raise ValueError("Dataset quality gate has not passed for this season")
    rows, hashes = read_corpus(output, season)
    if hashes != report.get("artifact_sha256"):
        raise ValueError("Dataset changed after validation; rerun the quality gate")
    return rows
