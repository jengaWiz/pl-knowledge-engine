"""Build a reviewable numerical oracle from raw CSVs using independent SQLite SQL.

This script does not import normalization, analysis or API implementation code.
The committed golden results are regenerated only when reviewing source contracts.
"""

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.sources import inspect_source, load_sources


def build_references(output: Path, season: str) -> dict:
    contracts = {source.id: source for source in load_sources(season)}
    base = output / "raw/mvp" / season
    used = {}

    def read(name, path):
        content = path.read_bytes()
        inspect_source(contracts[name], content)
        used[name] = contracts[name].sha256
        return list(csv.DictReader(content.decode("utf-8-sig").splitlines()))

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("CREATE TABLE fixtures (day, home, away, h INTEGER, a INTEGER, source_row INTEGER)")
    fixtures = read("matches", base / "matches.csv")
    for number, row in enumerate(fixtures, 2):
        day, month, year = row["Date"].split("/")
        year = "20" + year if len(year) == 2 else year
        db.execute(
            "INSERT INTO fixtures VALUES (?,?,?,?,?,?)",
            (
                f"{year}-{month.zfill(2)}-{day.zfill(2)}",
                row["HomeTeam"],
                row["AwayTeam"],
                int(row["FTHG"]),
                int(row["FTAG"]),
                number,
            ),
        )
    db.execute("""CREATE VIEW perspective AS
        SELECT day,home AS team,1 AS home,h AS gf,a AS ga,source_row FROM fixtures
        UNION ALL SELECT day,away,0,a,h,source_row FROM fixtures""")
    aggregate = """SELECT count(*) AS matches,
        sum(gf>ga) AS wins, sum(gf=ga) AS draws, sum(gf<ga) AS losses,
        sum(gf) AS goals_for, sum(ga) AS goals_against, sum(gf-ga) AS goal_difference,
        sum(CASE WHEN gf>ga THEN 3 WHEN gf=ga THEN 1 ELSE 0 END) AS points FROM ({query})"""
    cases = []
    for team in ("Aston Villa", "Liverpool"):
        query = "SELECT * FROM perspective WHERE team=?"
        full = dict(db.execute(aggregate.format(query=query), [team]).fetchone())
        refs = [
            row[0] for row in db.execute("SELECT source_row FROM perspective WHERE team=?", [team])
        ]
        for metric, value in full.items():
            cases.append(
                {
                    "question": f"{team} season {metric.replace('_', ' ')}?",
                    "request": {"operation": "team_stats", "teams": [team]},
                    "path": ["rows", 0, metric],
                    "expected": value,
                    "source_rows": sorted(refs),
                }
            )
        for home, split in ((1, "home"), (0, "away")):
            query = f"SELECT * FROM perspective WHERE team=? AND home={home}"
            total = dict(db.execute(aggregate.format(query=query), [team]).fetchone())
            split_refs = [row[0] for row in db.execute(query.replace("*", "source_row"), [team])]
            for metric in ("matches", "points", "goals_for", "goals_against"):
                cases.append(
                    {
                        "question": f"{team} {split} {metric.replace('_', ' ')}?",
                        "request": {"operation": "home_away", "teams": [team]},
                        "path": ["rows", 0, split, metric],
                        "expected": total[metric],
                        # The endpoint cites both splits; record all contributing fixtures.
                        "source_rows": sorted(refs),
                        "calculation_source_rows": sorted(split_refs),
                    }
                )
        recent_query = "SELECT * FROM perspective WHERE team=? ORDER BY day DESC LIMIT 5"
        recent = list(db.execute(recent_query, [team]))[::-1]
        form = "".join(
            "W" if row["gf"] > row["ga"] else "D" if row["gf"] == row["ga"] else "L"
            for row in recent
        )
        points = db.execute(aggregate.format(query=recent_query), [team]).fetchone()["points"]
        for metric, value in (("form", form), ("points", points)):
            cases.append(
                {
                    "question": f"{team} last five {metric}?",
                    "request": {"operation": "form", "teams": [team]},
                    "path": ["rows", 0, metric],
                    "expected": value,
                    "source_rows": sorted(row["source_row"] for row in recent),
                }
            )
    teams = read("teams", base / "teams.csv")
    codes = {row["name"]: int(row["code"]) for row in teams}
    db.execute("CREATE TABLE players (id INTEGER, code INTEGER, name)")
    for row in read("players", base / "players.csv"):
        db.execute(
            "INSERT INTO players VALUES (?,?,?)",
            (
                int(row["player_id"]),
                int(row["player_code"]),
                (row["first_name"] + " " + row["second_name"]).strip(),
            ),
        )
    db.execute("CREATE TABLE lineups (gw INTEGER, fixture, player INTEGER, team INTEGER)")
    db.execute(
        "CREATE TABLE appearances (gw INTEGER, fixture, player INTEGER, "
        "minutes INTEGER, goals INTEGER, assists INTEGER, source_row INTEGER)"
    )
    for week in range(1, 39):
        for row in read(f"gw_{week}_lineups", base / f"archive/gw_{week}_lineups.csv"):
            db.execute(
                "INSERT INTO lineups VALUES (?,?,?,?)",
                (week, row["match_id"], int(row["player_id"]), int(row["team_code"])),
            )
        for number, row in enumerate(
            read(f"gw_{week}_appearances", base / f"archive/gw_{week}_appearances.csv"), 2
        ):
            db.execute(
                "INSERT INTO appearances VALUES (?,?,?,?,?,?,?)",
                (
                    week,
                    row["match_id"],
                    int(row["player_id"]),
                    int(row["minutes_played"]),
                    int(row["goals"]) if row["goals"] else None,
                    int(row["assists"]) if row["assists"] else None,
                    number,
                ),
            )
    for team in ("Aston Villa", "Liverpool"):
        for metric in ("goals", "assists", "goals_per90"):
            column = metric.removesuffix("_per90")
            expression = (
                f"sum(a.{column})"
                if metric != "goals_per90"
                else "90.0*sum(a.goals)/sum(a.minutes)"
            )
            query = f"""SELECT p.name,p.code,{expression} AS value,sum(a.minutes) AS minutes
                FROM appearances a JOIN lineups l
                  ON a.gw=l.gw AND a.fixture=l.fixture AND a.player=l.player
                JOIN players p ON a.player=p.id WHERE l.team=?
                GROUP BY p.id HAVING count(a.{column})=count(*) AND sum(a.minutes)>=?
                ORDER BY value DESC,p.name,p.code LIMIT 3"""
            threshold = 450 if metric == "goals_per90" else 0
            ranking = [dict(row) for row in db.execute(query, [codes[team], threshold])]
            for row in ranking:
                row["player_id"] = f"pl:{season}:player:{row.pop('code')}"
                if metric == "goals_per90":
                    row["value"] = round(row["value"], 3)
            cases.append(
                {
                    "question": f"{team} top three {metric.replace('_', ' ')}?",
                    "request": {
                        "operation": "player_rankings",
                        "teams": [team],
                        "metric": metric,
                        "min_minutes": threshold,
                        "limit": 3,
                    },
                    "path": ["rows"],
                    "expected": ranking,
                    "projection": ["name", "player_id", "value", "minutes"],
                }
            )
    db.close()
    return {
        "season": season,
        "method": "Independent raw CSV SQLite oracle",
        "source_sha256": used,
        "cases": cases,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data"))
    parser.add_argument("--season", default="2025-26")
    parser.add_argument("--write", type=Path, required=True)
    args = parser.parse_args()
    report = build_references(args.output, args.season)
    args.write.parent.mkdir(parents=True, exist_ok=True)
    args.write.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"Built {len(report['cases'])} independent reference questions")


if __name__ == "__main__":
    main()
