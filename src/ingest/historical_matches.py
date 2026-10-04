"""Normalize complete historical Premier League evidence without inference."""

import csv
import io
import re
from collections import Counter
from datetime import datetime

from config.season import validate_season_dates

# Explicit publisher aliases. Cross-source reconciliation follows in MVP04.
TEAM_NAMES = {
    "Man City": "Manchester City",
    "Man United": "Manchester United",
    "Newcastle": "Newcastle United",
    "Nott'm Forest": "Nottingham Forest",
    "Tottenham": "Tottenham Hotspur",
    "West Ham": "West Ham United",
    "Wolves": "Wolverhampton Wanderers",
}
MATCH_STATS = {
    "HS": "home_shots",
    "AS": "away_shots",
    "HST": "home_shots_on_target",
    "AST": "away_shots_on_target",
    "HC": "home_corners",
    "AC": "away_corners",
    "HY": "home_yellow_cards",
    "AY": "away_yellow_cards",
    "HR": "home_red_cards",
    "AR": "away_red_cards",
    "HF": "home_fouls",
    "AF": "away_fouls",
}


def _integer(value: str | None, name: str, *, optional: bool = False) -> int | None:
    if value is None or not value.strip():
        if optional:
            return None
        raise ValueError(f"Missing completed-match field: {name}")
    if not re.fullmatch(r"\d+", value.strip()):
        raise ValueError(f"Invalid nonnegative integer for {name}: {value!r}")
    return int(value)


def normalize_matches(content: bytes, season: str, source_id: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    required = {"Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"}
    if not required <= set(reader.fieldnames or []):
        raise ValueError("Missing required match columns")
    records = []
    for row_number, row in enumerate(reader, 2):
        if not any(row.values()):
            continue
        if None in row or any(row.get(field) is None for field in required):
            raise ValueError(f"Row {row_number}: malformed CSV")
        if row["Div"] != "E0":
            raise ValueError(f"Row {row_number}: unsupported competition")
        home_raw, away_raw = (row[name].strip() for name in ("HomeTeam", "AwayTeam"))
        home, away = (TEAM_NAMES.get(name, name) for name in (home_raw, away_raw))
        if not home or not away or home == away:
            raise ValueError(f"Row {row_number}: invalid teams")
        date = datetime.strptime(row["Date"], "%d/%m/%Y").date().isoformat()
        validate_season_dates([date], season)
        home_score = _integer(row["FTHG"], "FTHG")
        away_score = _integer(row["FTAG"], "FTAG")
        expected = "H" if home_score > away_score else "A" if home_score < away_score else "D"
        if row["FTR"] != expected:
            raise ValueError(f"Row {row_number}: full-time result conflicts with scores")
        pair = ":".join(
            re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") for name in (home, away)
        )
        stats = {
            name: _integer(row.get(column), column, optional=True)
            for column, name in MATCH_STATS.items()
        }
        for side in ("home", "away"):
            shots, on_target = stats[f"{side}_shots"], stats[f"{side}_shots_on_target"]
            if shots is not None and on_target is not None and on_target > shots:
                raise ValueError(f"Row {row_number}: shots on target exceed shots")
        records.append(
            {
                "id": f"pl:{season}:{pair}",
                "season": season,
                "competition": "Premier League",
                "date": date,
                "home_team": home,
                "away_team": away,
                "home_score": home_score,
                "away_score": away_score,
                "result": expected,
                "finished": True,
                **stats,
                "source_id": source_id,
                "source_row": row_number,
                "source_home_team": home_raw,
                "source_away_team": away_raw,
            }
        )
    return sorted(records, key=lambda record: (record["date"], record["id"]))


def validate_coverage(records: list[dict]) -> dict:
    """Require a full 20-team double round robin, reporting all discrepancies."""
    teams = sorted({record[key] for record in records for key in ("home_team", "away_team")})
    counts = Counter(record[key] for record in records for key in ("home_team", "away_team"))
    pairs = Counter((record["home_team"], record["away_team"]) for record in records)
    issues = []
    if len(records) != 380:
        issues.append(f"Expected 380 completed fixtures; found {len(records)}")
    if len(teams) != 20:
        issues.append(f"Expected 20 teams; found {len(teams)}")
    bad_counts = {name: counts[name] for name in teams if counts[name] != 38}
    if bad_counts:
        issues.append(f"Teams without 38 fixtures: {bad_counts}")
    duplicate_pairs = [list(pair) for pair, count in sorted(pairs.items()) if count > 1]
    missing_pairs = [
        [home, away]
        for home in teams
        for away in teams
        if home != away and (home, away) not in pairs
    ]
    if duplicate_pairs:
        issues.append(f"Duplicate home/away pairs: {duplicate_pairs}")
    if missing_pairs:
        issues.append(f"Missing home/away pairs: {missing_pairs}")
    for name in ("Aston Villa", "Liverpool"):
        if name not in teams:
            issues.append(f"Missing required focus team: {name}")
    return {
        "valid": not issues,
        "fixtures": len(records),
        "teams": len(teams),
        "fixtures_by_team": dict(sorted(counts.items())),
        "issues": issues,
        "missing_statistics": {
            field: sum(record[field] is None for record in records)
            for field in MATCH_STATS.values()
        },
        "date_range": [
            min(record["date"] for record in records),
            max(record["date"] for record in records),
        ]
        if records
        else None,
    }
