"""Join archived player evidence to canonical matches, using actual lineups."""

import csv
import io
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation

from config.sources import SourceSpec
from src.ingest.historical_matches import TEAM_NAMES

FOCUS_TEAMS = {"Aston Villa", "Liverpool"}


def source_rows(content: bytes, source: SourceSpec) -> list[dict]:
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    rows = []
    for number, row in enumerate(reader, 2):
        if not any(row.values()):
            continue
        if None in row:
            raise ValueError(f"{source.id} row {number}: malformed CSV")
        rows.append(
            {
                **row,
                "_source": {
                    "id": source.id,
                    "url": source.url,
                    "sha256": source.sha256,
                    "revision": source.revision,
                    "row": number,
                },
            }
        )
    return rows


def number(value: str | None, *, integer: bool = False, optional: bool = False):
    if value is None or not str(value).strip():
        if optional:
            return None
        raise ValueError("Missing required numeric evidence")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid numeric evidence: {value!r}") from exc
    if not parsed.is_finite() or parsed < 0 or (integer and parsed != parsed.to_integral_value()):
        raise ValueError(f"Invalid nonnegative {'integer' if integer else 'number'}: {value!r}")
    return int(parsed) if integer else float(parsed)


def build_player_corpus(
    teams: list[dict],
    players: list[dict],
    snapshots: list[dict],
    archive_matches: list[dict],
    lineups: list[dict],
    appearances: list[dict],
    canonical_matches: list[dict],
    season: str,
) -> tuple[list[dict], list[dict], dict]:
    codes = {}
    for row in teams:
        code = number(row["code"], integer=True)
        name = TEAM_NAMES.get(row["name"], row["name"])
        if code in codes:
            raise ValueError("Duplicate archive team code")
        codes[code] = name
    if not FOCUS_TEAMS <= set(codes.values()):
        raise ValueError("Missing focus teams in archived team metadata")

    identities = {}
    for row in players:
        key = number(row["player_id"], integer=True)
        if key in identities:
            raise ValueError("Duplicate archive player identifier")
        identities[key] = row

    latest = {}
    for row in snapshots:
        key = number(row["id"], integer=True)
        gw = number(row["gw"], integer=True)
        if not 1 <= gw <= 38:
            raise ValueError("Player snapshot gameweek outside the season")
        if key not in latest or gw > number(latest[key]["gw"], integer=True):
            latest[key] = row
        elif gw == number(latest[key]["gw"], integer=True) and row != latest[key]:
            raise ValueError("Conflicting player snapshots for the same gameweek")

    canonical = {(row["home_team"], row["away_team"]): row for row in canonical_matches}
    matched = {}
    gameweeks = {}
    fixture_conflicts = []
    for row in archive_matches:
        if row["tournament"] != "prem" or row["finished"].lower() != "true":
            raise ValueError("Archive includes an unfinished or non-PL match")
        home, away = (
            codes[number(row[field], integer=True)] for field in ("home_team", "away_team")
        )
        match = canonical.get((home, away))
        if not match:
            raise ValueError(f"Archive fixture cannot be reconciled: {home} v {away}")
        day = datetime.fromisoformat(row["kickoff_time"]).date().isoformat()
        if day != match["date"]:
            raise ValueError(f"Archive fixture date disagrees with primary result: {match['id']}")
        archive_scores = [
            number(row[field], integer=True) for field in ("home_score", "away_score")
        ]
        primary_scores = [match["home_score"], match["away_score"]]
        if archive_scores != primary_scores:
            fixture_conflicts.append(
                {
                    "match_id": match["id"],
                    "primary_scores": primary_scores,
                    "archive_scores": archive_scores,
                    "archive_source": row["_source"],
                    "resolution": (
                        "Primary Football-Data result takes precedence; joined by teams and date"
                    ),
                }
            )
        key = row["match_id"]
        if key in matched:
            raise ValueError("Duplicate archived fixture identifier")
        matched[key] = match
        gameweeks[key] = number(row["gameweek"], integer=True)
    if len(matched) != len(canonical):
        raise ValueError("Archive match coverage differs from the canonical league schedule")

    lineup_map = {}
    expected_focus = Counter()
    for row in lineups:
        match = matched.get(row["match_id"])
        if match is None:
            raise ValueError("Lineup refers to an unmatched fixture")
        team = codes[number(row["team_code"], integer=True)]
        if team not in (match["home_team"], match["away_team"]):
            raise ValueError("Lineup team does not participate in its fixture")
        key = (row["match_id"], number(row["player_id"], integer=True))
        if key in lineup_map:
            raise ValueError("Duplicate lineup player/fixture")
        lineup_map[key] = (team, row)
        if team in FOCUS_TEAMS:
            expected_focus[team] += 1

    records = []
    clubs = defaultdict(set)
    appearance_keys = set()
    missing_lineups = []
    unknown_players = []
    for row in appearances:
        player_key = number(row["player_id"], integer=True)
        key = (row["match_id"], player_key)
        if key in appearance_keys:
            raise ValueError("Duplicate player/fixture appearance")
        appearance_keys.add(key)
        if key not in lineup_map:
            match = matched.get(key[0])
            if match is None:
                raise ValueError("Appearance refers to an unmatched fixture")
            missing_lineups.append(
                {
                    "match_id": key[0],
                    "player_id": key[1],
                    "focus_fixture": bool(
                        FOCUS_TEAMS.intersection({match["home_team"], match["away_team"]})
                    ),
                }
            )
            continue
        team, lineup = lineup_map[key]
        if team not in FOCUS_TEAMS:
            continue
        identity = identities.get(player_key)
        if identity is None:
            unknown_players.append(player_key)
            continue
        player_id = f"pl:{season}:player:{identity['player_code']}"
        minutes = number(row["minutes_played"], integer=True)
        if minutes > 130:
            raise ValueError("Implausible minutes_played; review the archive record")
        match = matched[key[0]]
        records.append(
            {
                "id": f"{player_id}:{match['id']}",
                "season": season,
                "player_id": player_id,
                "source_player_id": player_key,
                "match_id": match["id"],
                "team": team,
                "date": match["date"],
                "gameweek": gameweeks[key[0]],
                "minutes": minutes,
                "goals": number(row["goals"], integer=True, optional=True),
                "assists": number(row["assists"], integer=True, optional=True),
                "expected_goals": number(row.get("xg"), optional=True),
                "expected_assists": number(row.get("xa"), optional=True),
                "fpl_points": None,
                "clean_sheets": None,
                "team_attribution": "match_lineup",
                "source": row["_source"],
                "lineup_source": lineup["_source"],
                "match_source_id": row["match_id"],
            }
        )
        clubs[player_key].add(team)

    roster = []
    for player_key in sorted(clubs):
        identity = identities[player_key]
        snapshot = latest.get(player_key)
        roster.append(
            {
                "id": f"pl:{season}:player:{identity['player_code']}",
                "season": season,
                "source_player_id": player_key,
                "player_code": identity["player_code"],
                "name": f"{identity['first_name']} {identity['second_name']}".strip(),
                "web_name": identity["web_name"],
                "position": identity["position"],
                "teams_represented": sorted(clubs[player_key]),
                "source": identity["_source"],
                "fpl_season_totals": {
                    field: number(
                        snapshot.get(field),
                        optional=True,
                        integer=field not in {"expected_goals", "expected_assists"},
                    )
                    for field in [
                        "minutes",
                        "goals_scored",
                        "assists",
                        "total_points",
                        "clean_sheets",
                        "expected_goals",
                        "expected_assists",
                    ]
                }
                if snapshot
                else None,
                "fpl_snapshot_gameweek": number(snapshot["gw"], integer=True) if snapshot else None,
                "fpl_source": snapshot["_source"] if snapshot else None,
                "fpl_scope": "Player season across all clubs; not summed gameweek snapshots",
            }
        )

    coverage = defaultdict(set)
    for row in records:
        coverage[row["team"]].add(row["match_id"])
    lineups_without_stats = [
        {
            "match_id": key[0],
            "player_id": key[1],
            "team": team,
            "starting": row.get("is_starting", "").lower() == "true",
        }
        for key, (team, row) in lineup_map.items()
        if team in FOCUS_TEAMS and key not in appearance_keys
    ]
    report = {
        "valid": not any(row["focus_fixture"] for row in missing_lineups)
        and not any(row["starting"] for row in lineups_without_stats)
        and not unknown_players
        and all(len(coverage[team]) == 38 for team in FOCUS_TEAMS),
        "fixture_conflicts": fixture_conflicts,
        "players": len(roster),
        "appearance_records": len(records),
        "played_appearances": sum(row["minutes"] > 0 for row in records),
        "team_fixture_coverage": {team: len(coverage[team]) for team in sorted(FOCUS_TEAMS)},
        "lineup_records_by_team": dict(expected_focus),
        "missing_lineups": missing_lineups,
        "lineups_without_stats": lineups_without_stats,
        "unknown_players": sorted(set(unknown_players)),
        "metric_missingness": {
            field: sum(row[field] is None for row in records)
            for field in [
                "goals",
                "assists",
                "expected_goals",
                "expected_assists",
                "fpl_points",
                "clean_sheets",
            ]
        },
        "definitions": {
            "appearance": (
                "Archive player-match record; may include unused bench players (minutes=0)"
            ),
            "assists": "Archive match-stat provider assists, distinct from FPL awarded assists",
            "fpl_points": "Unavailable per match; only latest season snapshot retained separately",
            "minutes": "minutes_played; never inferred from start_min/finish_min",
        },
    }
    return (
        sorted(roster, key=lambda row: row["id"]),
        sorted(records, key=lambda row: row["id"]),
        report,
    )
