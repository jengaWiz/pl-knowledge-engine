"""Compute descriptive statistics from verified records, never inferred claims."""

import re
from pathlib import Path

from src.clean.corpus_quality import load_verified_corpus
from src.ingest.commentary import FOCUS

OPERATIONS = {"team_stats", "form", "home_away", "player_rankings"}
PLAYER_METRICS = {"goals", "assists", "minutes", "goals_per90", "assists_per90"}
DEFINITIONS = {
    "team_stats": "Points = 3 × wins + draws from match results; "
    "excludes administrative deductions. "
    "Goal difference = goals for − goals against. These are team match totals, not FPL totals.",
    "form": "Last five completed league matches ordered by date then fixture ID; "
    "W/D/L and points are from the named team's perspective.",
    "home_away": "Separate league result totals at home and away; "
    "points per game = result points / matches in that split.",
    "player_rankings": "Archive match-stat goals/assists within retained focus-club appearances. "
    "Archive assists differ from FPL assists; cumulative FPL snapshots are never summed. "
    "Per 90 = 90 × metric / recorded minutes, with an explicit minimum-minutes threshold.",
}


class AnalysisInputError(ValueError):
    """Requested operation is outside verified metric/team support."""


def result_rows(matches, team):
    rows = []
    for match in matches:
        if team not in (match["home_team"], match["away_team"]):
            continue
        home = match["home_team"] == team
        scored = match["home_score" if home else "away_score"]
        conceded = match["away_score" if home else "home_score"]
        rows.append(
            {
                "id": match["id"],
                "date": match["date"],
                "home": home,
                "goals_for": scored,
                "goals_against": conceded,
                "result": "W" if scored > conceded else "D" if scored == conceded else "L",
                "points": 3 if scored > conceded else 1 if scored == conceded else 0,
            }
        )
    return sorted(rows, key=lambda row: (row["date"], row["id"]))


def totals(rows):
    games = len(rows)
    scored = sum(row["goals_for"] for row in rows)
    conceded = sum(row["goals_against"] for row in rows)
    points = sum(row["points"] for row in rows)
    return {
        "matches": games,
        "wins": sum(row["result"] == "W" for row in rows),
        "draws": sum(row["result"] == "D" for row in rows),
        "losses": sum(row["result"] == "L" for row in rows),
        "goals_for": scored,
        "goals_against": conceded,
        "goal_difference": scored - conceded,
        "points": points,
        "points_per_game": round(points / games, 3) if games else None,
    }


def sources(records, kind):
    grouped = {}
    for row in records:
        ref = (
            row["source"]
            if kind in ("appearance", "player")
            else {
                "url": row["source_url"],
                "sha256": row["source_sha256"],
                "row": row["source_row"],
            }
        )
        entry = grouped.setdefault(
            ref["url"],
            {
                "type": "player_identity" if kind == "player" else "statistics",
                "url": ref["url"],
                "source_sha256": ref["sha256"],
                "record_ids": [],
                "source_rows": [],
                "summary": "Verified source records used in this calculation",
            },
        )
        entry["record_ids"].append(row["id"])
        entry["source_rows"].append(ref["row"])
    return list(grouped.values())


def analyze(
    output: Path,
    season: str,
    operation: str,
    *,
    teams=None,
    metric="goals",
    min_minutes=0,
    limit=10,
) -> dict:
    if operation not in OPERATIONS or metric not in PLAYER_METRICS:
        raise AnalysisInputError("Unsupported operation or player metric")
    if not 0 <= min_minutes <= 10000 or not 1 <= limit <= 100:
        raise AnalysisInputError("Invalid minimum minutes or result limit")
    corpus = load_verified_corpus(output, season)
    available = {row[key] for row in corpus["matches"] for key in ("home_team", "away_team")}
    teams = list(dict.fromkeys(teams or FOCUS))
    if any(team not in available for team in teams):
        raise AnalysisInputError("Requested team is outside the verified league corpus")
    rows = []
    evidence = []
    excluded = {"below_minimum_minutes": 0, "missing_metric": 0}
    if operation == "player_rankings":
        if any(team not in FOCUS for team in teams):
            raise AnalysisInputError(
                "Player appearance coverage is limited to Aston Villa and Liverpool"
            )
        rate = metric.endswith("_per90")
        field = metric.removesuffix("_per90")
        if rate and min_minutes < 1:
            raise AnalysisInputError("Per-90 rankings require a positive minimum-minutes threshold")
        selected = [row for row in corpus["appearances"] if row["team"] in teams]
        evidence = selected
        for player in corpus["players"]:
            apps = [row for row in selected if row["player_id"] == player["id"]]
            if not apps:
                continue
            minutes = sum(row["minutes"] for row in apps)
            if minutes < min_minutes:
                excluded["below_minimum_minutes"] += 1
                continue
            if any(row.get(field) is None for row in apps):
                excluded["missing_metric"] += 1
                continue
            value = sum(row[field] for row in apps)
            rows.append(
                {
                    "player_id": player["id"],
                    "name": player["name"],
                    "teams": sorted({row["team"] for row in apps}),
                    "value": 90 * value / minutes if rate else value,
                    "minutes": minutes,
                    "appearances": sum(row["minutes"] > 0 for row in apps),
                }
            )
        rows.sort(key=lambda row: (-row["value"], row["name"], row["player_id"]))
        rows = rows[:limit]
        if rate:
            for row in rows:
                row["value"] = round(row["value"], 3)
    else:
        ids = set()
        for team in teams:
            games = result_rows(corpus["matches"], team)
            if operation == "form":
                games = games[-5:]
                rows.append(
                    {
                        "team": team,
                        **totals(games),
                        "form": "".join(row["result"] for row in games),
                        "fixtures": games,
                    }
                )
            elif operation == "home_away":
                rows.append(
                    {
                        "team": team,
                        "home": totals([r for r in games if r["home"]]),
                        "away": totals([r for r in games if not r["home"]]),
                    }
                )
            else:
                rows.append({"team": team, **totals(games)})
            ids.update(row["id"] for row in games)
        evidence = [row for row in corpus["matches"] if row["id"] in ids]
    references = sources(evidence, "appearance" if operation == "player_rankings" else "match")
    if operation == "player_rankings":
        ids = {row["player_id"] for row in evidence}
        identities = [row for row in corpus["players"] if row["id"] in ids]
        references.extend(sources(identities, "player"))
    dates = sorted(row["date"] for row in evidence)
    return {
        "status": "ok" if rows else "insufficient_evidence",
        "season": season,
        "operation": operation,
        "teams": teams,
        "metric": metric if operation == "player_rankings" else None,
        "minimum_minutes": min_minutes,
        "rows": rows,
        "definition": DEFINITIONS[operation],
        "sample_size": len(evidence),
        "excluded_players": excluded,
        "date_range": {"from": dates[0], "to": dates[-1]} if dates else None,
        "sources": references,
        "limitations": [
            "Descriptive results do not establish causes or predict future matches.",
            "Missing player metrics are excluded rather than replaced with zero.",
        ],
    }


def answer(output: Path, season: str, message: str) -> dict:
    """Recognize a bounded vocabulary; unsupported questions abstain explicitly."""
    text = message.casefold()
    insufficient = {
        "status": "insufficient_evidence",
        "season": season,
        "sources": [],
        "reply": f"I cannot establish that from the verified {season} records. "
        "Ask about team points/goal difference, last-five form, home versus away, "
        "or focus-club goal/assist rankings and per-90 rates. "
        "Causes, predictions, injuries and external opinions require additional evidence.",
    }
    seasons = re.findall(r"\b(20\d{2})[–/-](\d{2}|20\d{2})\b", text)
    if any(f"{year}-{end[-2:]}" != season for year, end in seasons):
        return insufficient
    if re.search(r"\b(why|predict|prediction|will|injur\w*|podcast|opinion|captions?|fpl)\b", text):
        return insufficient
    last = re.search(r"last\s+(\d+)\s+(?:games|matches)", text)
    if (last and last[1] != "5") or "next season" in text or "next year" in text:
        return insufficient
    corpus = load_verified_corpus(output, season)
    available = sorted(
        {row[key] for row in corpus["matches"] for key in ("home_team", "away_team")}
    )
    teams = [
        team for team in available if re.search(r"\b" + re.escape(team.casefold()) + r"\b", text)
    ]
    if "Aston Villa" not in teams and re.search(r"\bvilla\b", text):
        teams.append("Aston Villa")
    if any(term in text for term in ("clean sheet", "yellow card", "red card", "expected goal")):
        return insufficient
    player = bool(re.search(r"\b(scorers?|players?|assists?|per.?90)\b", text))
    if player and not re.search(r"scorers?|goals?|assists?|most minutes|per.?90", text):
        return insufficient
    if player and not teams and not re.search(r"both teams|both clubs|which players", text):
        return insufficient
    if player:
        operation = "player_rankings"
        metric = "assists" if "assist" in text else "minutes" if "most minutes" in text else "goals"
        if re.search(r"per.?90", text):
            metric += "_per90"
        threshold = re.search(r"(?:at least|minimum)\s+(\d+)\s+minutes", text)
        minimum = int(threshold[1]) if threshold else 450 if metric.endswith("per90") else 0
    elif teams and re.search(r"\b(last.?5|last.?five|form)\b", text):
        operation, metric, minimum = "form", "goals", 0
    elif teams and "home" in text and "away" in text:
        operation, metric, minimum = "home_away", "goals", 0
    elif teams and re.search(
        r"\b(points|goal difference|goals|statistics|stats|performed)\b", text
    ):
        operation, metric, minimum = "team_stats", "goals", 0
    else:
        return insufficient
    try:
        result = analyze(output, season, operation, teams=teams, metric=metric, min_minutes=minimum)
    except AnalysisInputError:
        return insufficient
    if result["status"] != "ok":
        return insufficient
    lines = [
        f"**{season} · {operation.replace('_', ' ')}**",
        f"Teams: {', '.join(result['teams'])}.",
    ]
    for row in result["rows"]:
        if operation == "player_rankings":
            lines.append(
                f"- {row['name']}: **{row['value']} {metric.replace('_', ' ')}**, "
                f"{row['minutes']} minutes, {row['appearances']} appearances."
            )
        elif operation == "home_away":
            lines.append(
                f"- {row['team']}: home **{row['home']['points']} points** "
                f"in {row['home']['matches']} matches; away **{row['away']['points']} points** "
                f"in {row['away']['matches']} matches."
            )
        else:
            form = f" Form: **{row['form']}** (oldest to newest)." if operation == "form" else ""
            lines.append(
                f"- {row['team']}: **{row['points']} points**, "
                f"{row['goals_for']} goals for, {row['goals_against']} against, "
                f"**{row['goal_difference']:+} goal difference**, {row['matches']} matches.{form}"
            )
    lines.extend(
        [
            "",
            result["definition"],
            f"Sample: {result['sample_size']} verified records; "
            f"{result['date_range']['from']} to {result['date_range']['to']}.",
            f"Minimum minutes: {minimum}." if player else "",
        ]
    )
    return {**result, "reply": "\n".join(lines)}
