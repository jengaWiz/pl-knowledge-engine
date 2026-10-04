"""Load only quality-verified, season-scoped MVP nodes in one graph
transaction."""

import csv
import json
from pathlib import Path

from neo4j import GraphDatabase

from config.sources import inspect_source, load_sources
from src.clean.corpus_quality import load_verified_corpus
from src.ingest.historical_matches import TEAM_NAMES
from src.ingest.historical_players import number


def fixture_gameweeks(output: Path, season: str, matches: list[dict]) -> dict:
    sources = {source.id: source for source in load_sources(season)}
    base = output / "raw" / "mvp" / season
    content = (base / "teams.csv").read_bytes()
    inspect_source(sources["teams"], content)
    teams = {
        int(row["code"]): TEAM_NAMES.get(row["name"], row["name"])
        for row in csv.DictReader(content.decode().splitlines())
    }
    pairs = {(match["home_team"], match["away_team"]): match["id"] for match in matches}
    result = {}
    for source in sources.values():
        if not source.id.startswith("gw_") or not source.id.endswith("_matches"):
            continue
        content = (base / "archive" / f"{source.id}.csv").read_bytes()
        inspect_source(source, content)
        for row in csv.DictReader(content.decode().splitlines()):
            pair = tuple(
                teams[number(row[field], integer=True)] for field in ("home_team", "away_team")
            )
            ident = pairs[pair]
            if ident in result:
                raise ValueError("Duplicate graph gameweek mapping")
            result[ident] = number(row["gameweek"], integer=True)
    if len(result) != len(matches):
        raise ValueError("Incomplete graph gameweek mapping")
    return result


def load_graph(output: Path, season: str, uri: str, user: str, password: str) -> dict:
    if not password:
        raise ValueError("Graph loading requires NEO4J_PASSWORD")
    corpus = load_verified_corpus(output, season)
    manifest = json.loads((output / "reports" / "mvp" / season / "quality.json").read_text())
    dataset = ":".join(manifest["artifact_sha256"][name] for name in sorted(corpus))
    gameweeks = fixture_gameweeks(output, season, corpus["matches"])
    teams = sorted(
        {match[key] for match in corpus["matches"] for key in ("home_team", "away_team")}
    )
    totals = {}
    for player in corpus["players"]:
        apps = [row for row in corpus["appearances"] if row["player_id"] == player["id"]]
        totals[player["id"]] = {
            "goals_scored": sum(row["goals"] for row in apps if row["goals"] is not None),
            "assists": sum(row["assists"] for row in apps if row["assists"] is not None),
            "minutes": sum(row["minutes"] for row in apps),
            "expected_goals": sum(
                row["expected_goals"] for row in apps if row["expected_goals"] is not None
            ),
            "expected_assists": sum(
                row["expected_assists"] for row in apps if row["expected_assists"] is not None
            ),
            "total_points": (player.get("fpl_season_totals") or {}).get("total_points"),
        }
    common = {"season": season, "mvp_managed": True, "dataset_id": dataset}
    player_rows = [
        {
            "id": player["id"],
            "teams": player["teams_represented"],
            "props": {
                **common,
                "web_name": player["web_name"],
                "full_name": player["name"],
                "position": player["position"],
                **totals[player["id"]],
                "metric_scope": (
                    "Focus-club match-stat appearances; FPL points are separate season totals"
                ),
                "source_json": json.dumps(player["source"]),
            },
        }
        for player in corpus["players"]
    ]
    match_rows = [
        {
            "id": match["id"],
            "gw_id": f"pl:{season}:gw:{gameweeks[match['id']]}",
            "props": {
                **common,
                "date": match["date"],
                "gameweek": gameweeks[match["id"]],
                "home_team_name": match["home_team"],
                "away_team_name": match["away_team"],
                "home_score": match["home_score"],
                "away_score": match["away_score"],
                "source_url": match["source_url"],
                "source_sha256": match["source_sha256"],
            },
        }
        for match in corpus["matches"]
    ]
    appearance_rows = [
        {
            "id": row["id"],
            "player_id": row["player_id"],
            "match_id": row["match_id"],
            "props": {
                **common,
                "name": f"{row['team']} · {row['date']}",
                "team": row["team"],
                "gw": row["gameweek"],
                "minutes": row["minutes"],
                "goals_scored": row["goals"],
                "assists": row["assists"],
                "expected_goals": row["expected_goals"],
                "expected_assists": row["expected_assists"],
                "source_json": json.dumps(row["source"]),
            },
        }
        for row in corpus["appearances"]
    ]
    expected = {
        "Season": 1,
        "Team": len(teams),
        "Gameweek": len(set(gameweeks.values())),
        "Player": len(player_rows),
        "Match": len(match_rows),
        "PlayerAppearance": len(appearance_rows),
    }

    def write(tx):
        tx.run(
            "MERGE (s:Season {mvp_key: $season_key}) SET s += $props",
            season_key=f"pl:season:{season}",
            props={**common, "label": season},
        ).consume()
        tx.run(
            """UNWIND $teams AS name MERGE (t:Team {mvp_key:$season+":team:"+name}) SET
t += $props,t.name=name WITH t MATCH (s:Season {mvp_key:$season_key})
MERGE (t)-[r:IN_SEASON]->(s) SET r += $props""",
            teams=teams,
            props=common,
            season=season,
            season_key=f"pl:season:{season}",
        ).consume()
        tx.run(
            """UNWIND $weeks AS gw MERGE (g:Gameweek {mvp_key:gw.id}) SET g += $props,
g.number=gw.number WITH g MATCH (s:Season {mvp_key:$season_key}) MERGE
(g)-[r:PART_OF]->(s) SET r += $props""",
            weeks=[
                {"id": f"pl:{season}:gw:{gw}", "number": gw}
                for gw in sorted(set(gameweeks.values()))
            ],
            props=common,
            season_key=f"pl:season:{season}",
        ).consume()
        tx.run(
            """UNWIND $rows AS row MERGE (p:Player {player_id:row.id}) SET p +=
row.props WITH p,row UNWIND row.teams AS team MATCH (t:Team
{mvp_key:$season+":team:"+team}) MERGE (p)-[r:PLAYS_FOR]->(t) SET r +=
$props""",
            rows=player_rows,
            season=season,
            props=common,
        ).consume()
        tx.run(
            """UNWIND $rows AS row MERGE (m:Match {match_id:row.id}) SET m += row.props
WITH m,row MATCH (home:Team
{mvp_key:$season+":team:"+row.props.home_team_name}), (away:Team
{mvp_key:$season+":team:"+row.props.away_team_name}), (g:Gameweek
{mvp_key:row.gw_id}) MERGE (home)-[h:HOME_TEAM]->(m) SET h += $props
MERGE (away)-[a:AWAY_TEAM]->(m) SET a += $props MERGE
(m)-[w:PART_OF]->(g) SET w += $props""",
            rows=match_rows,
            season=season,
            props=common,
        ).consume()
        tx.run(
            """UNWIND $rows AS row MERGE (a:PlayerAppearance {appearance_id:row.id})
SET a += row.props WITH a,row MATCH (p:Player
{player_id:row.player_id}), (m:Match {match_id:row.match_id}) MERGE
(p)-[h:HAD_APPEARANCE]->(a) SET h += $props MERGE (a)-[i:IN_MATCH]->(m)
SET i += $props""",
            rows=appearance_rows,
            props=common,
        ).consume()
        tx.run(
            """MATCH (n) WHERE n.mvp_managed=true AND n.season=$season AND
n.dataset_id<>$dataset DETACH DELETE n""",
            season=season,
            dataset=dataset,
        ).consume()
        tx.run(
            (
                "MATCH ()-[r]->() WHERE r.mvp_managed=true AND r.season=$season AND "
                "r.dataset_id<>$dataset DELETE r "
            ),
            season=season,
            dataset=dataset,
        ).consume()
        counts = {
            row["label"]: row["count"]
            for row in tx.run(
                (
                    "MATCH (n) WHERE n.mvp_managed=true AND n.season=$season RETURN "
                    "labels(n)[0] AS label,count(n) AS count "
                ),
                season=season,
            )
        }
        if counts != expected:
            raise ValueError(f"Graph node count gate failed: {counts}")
        dangling = tx.run(
            """MATCH (a:PlayerAppearance) WHERE a.mvp_managed=true AND a.season=$season
AND (NOT EXISTS {(a)<-[:HAD_APPEARANCE]-(:Player)} OR NOT EXISTS
{(a)-[:IN_MATCH]->(:Match)}) RETURN count(a) AS count""",
            season=season,
        ).single()["count"]
        if dangling:
            raise ValueError("Graph has dangling appearance relationships")
        relationship_count = tx.run(
            (
                "MATCH ()-[r]->() WHERE r.mvp_managed=true AND r.season=$season RETURN "
                "count(r) AS count "
            ),
            season=season,
        ).single()["count"]
        expected_relationships = (
            len(teams)
            + expected["Gameweek"]
            + sum(len(p["teams"]) for p in player_rows)
            + len(match_rows) * 3
            + len(appearance_rows) * 2
        )
        if relationship_count != expected_relationships:
            raise ValueError("Graph relationship count gate failed")
        return counts

    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        driver.verify_connectivity()
        with driver.session() as session:
            for label, key in [
                ("Season", "mvp_key"),
                ("Team", "mvp_key"),
                ("Gameweek", "mvp_key"),
                ("Player", "player_id"),
                ("Match", "match_id"),
                ("PlayerAppearance", "appearance_id"),
            ]:
                session.run(
                    f"CREATE CONSTRAINT mvp_{label}_{key} IF NOT EXISTS "
                    f"FOR (n:{label}) REQUIRE n.{key} IS UNIQUE"
                ).consume()
            counts = session.execute_write(write)
    return {"valid": True, "season": season, "counts": counts, "dataset_id": dataset}
