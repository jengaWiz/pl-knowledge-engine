"""
Premier League Knowledge Engine — FastAPI backend.

Endpoints:
    GET  /api/graph/overview
    GET  /api/graph/player/{web_name}
    GET  /api/graph/match/{match_id}
    GET  /api/stats/top-players
    GET  /api/matches
    POST /api/chat
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Literal

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from config.settings import settings
from src.store.neo4j_store import Neo4jStore
from src.utils.logger import get_logger

logger = get_logger(__name__)

ALLOWED_STATS = {"goals_scored": "goals", "assists": "assists", "minutes": "minutes",
                 "goals_per90": "goals_per90", "assists_per90": "assists_per90"}


app = FastAPI(title="PL Knowledge Engine", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_neo4j() -> Neo4jStore:
    settings.require_credentials("neo4j_password")
    return Neo4jStore(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)


def _query_graph(store: Neo4jStore, cypher: str, params: dict[str, Any] | None = None) -> list[dict]:
    with store.driver.session() as session:
        result = session.run(cypher, **(params or {}))
        return result.data()


# ---------------------------------------------------------------------------
# Graph endpoints
# ---------------------------------------------------------------------------

@app.get("/api/graph/overview")
def graph_overview() -> dict[str, Any]:
    """Season overview subgraph — separate queries per entity type to avoid Cartesian products."""
    store = _get_neo4j()
    try:
        nodes: list[dict] = []
        seen: set = set()

        def add(item: dict) -> None:
            nid = item.get("id")
            if nid is not None and nid not in seen:
                seen.add(nid)
                nodes.append(item)

        # Season
        for r in _query_graph(store, "MATCH (s:Season) RETURN elementId(s) AS id, s.label AS name, 'Season' AS type"):
            add(r)

        # Both teams
        for r in _query_graph(store, """
            MATCH (t:Team)
            RETURN elementId(t) AS id, t.name AS name, 'Team' AS type, t.abbreviation AS abbreviation
        """):
            add(r)

        # Stadiums
        for r in _query_graph(store, """
            MATCH (st:Stadium)
            RETURN elementId(st) AS id, st.name AS name, 'Stadium' AS type
        """):
            add(r)

        # Top 15 players per team by goals + assists (avoids Cartesian product with matches)
        for r in _query_graph(store, """
            MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
            WITH t, p ORDER BY (coalesce(p.goals_scored, 0) + coalesce(p.assists, 0)) DESC
            WITH t, collect(p)[..15] AS top_players
            UNWIND top_players AS p
            MATCH (p)-[:PLAYS_FOR]->(t)
            RETURN elementId(p) AS id, p.web_name AS name, 'Player' AS type,
                   p.position AS position, p.goals_scored AS goals,
                   p.assists AS assists, t.name AS team
        """):
            add(r)

        # All matches (74 total — manageable)
        for r in _query_graph(store, """
            MATCH (m:Match)
            RETURN elementId(m) AS id, m.match_id AS name, 'Match' AS type,
                   m.home_team_name AS home, m.away_team_name AS away, m.gameweek AS gw
            ORDER BY m.gameweek ASC
        """):
            add(r)

        # All gameweeks
        for r in _query_graph(store, """
            MATCH (g:Gameweek)
            RETURN elementId(g) AS id, toString(g.number) AS name, 'Gameweek' AS type, g.number AS number
            ORDER BY g.number ASC
        """):
            add(r)

        # Edges — filter to only nodes we've included
        node_ids = {n["id"] for n in nodes}
        edge_rows = _query_graph(store, """
            MATCH (t:Team)-[:IN_SEASON]->(s:Season)
            RETURN elementId(t) AS source, elementId(s) AS target, 'IN_SEASON' AS type
            UNION
            MATCH (t:Team)-[:PLAYS_AT]->(st:Stadium)
            RETURN elementId(t) AS source, elementId(st) AS target, 'PLAYS_AT' AS type
            UNION
            MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
            RETURN elementId(p) AS source, elementId(t) AS target, 'PLAYS_FOR' AS type
            UNION
            MATCH (t:Team)-[:HOME_TEAM]->(m:Match)
            RETURN elementId(t) AS source, elementId(m) AS target, 'HOME_TEAM' AS type
            UNION
            MATCH (t:Team)-[:AWAY_TEAM]->(m:Match)
            RETURN elementId(t) AS source, elementId(m) AS target, 'AWAY_TEAM' AS type
            UNION
            MATCH (m:Match)-[:PART_OF]->(g:Gameweek)
            RETURN elementId(m) AS source, elementId(g) AS target, 'PART_OF' AS type
            UNION
            MATCH (g:Gameweek)-[:PART_OF]->(s:Season)
            RETURN elementId(g) AS source, elementId(s) AS target, 'PART_OF' AS type
        """)
        edges = [
            {"source": r["source"], "target": r["target"], "type": r["type"]}
            for r in edge_rows
            if r["source"] in node_ids and r["target"] in node_ids
        ]

        return {"nodes": nodes, "edges": edges}
    finally:
        store.close()


@app.get("/api/graph/player/{web_name}")
def graph_player(web_name: str) -> dict[str, Any]:
    """Subgraph for a single player."""
    store = _get_neo4j()
    try:
        rows = _query_graph(store, """
            MATCH (p:Player {web_name: $web_name})-[:PLAYS_FOR]->(t:Team)-[:IN_SEASON]->(s:Season)
            OPTIONAL MATCH (p)-[:HAD_APPEARANCE]->(a:PlayerAppearance)-[:IN_MATCH]->(m:Match)-[:PART_OF]->(g:Gameweek)
            RETURN
              {id: elementId(p), name: p.web_name, type: 'Player',
               goals: p.goals_scored, assists: p.assists, position: p.position,
               total_points: p.total_points, form: p.form} AS player,
              {id: elementId(t), name: t.name, type: 'Team'} AS team,
              {id: elementId(s), name: s.label, type: 'Season'} AS season,
              collect(DISTINCT {id: elementId(a), name: a.name, type: 'PlayerAppearance',
                gw: a.gw, goals: a.goals_scored, assists: a.assists, points: a.total_points}) AS appearances,
              collect(DISTINCT {id: elementId(m), name: m.match_id, type: 'Match',
                home: m.home_team_name, away: m.away_team_name}) AS matches,
              collect(DISTINCT {id: elementId(g), name: toString(g.number), type: 'Gameweek', number: g.number}) AS gameweeks
        """, {"web_name": web_name})

        if not rows:
            raise HTTPException(404, f"Player '{web_name}' not found")

        nodes: list[dict] = []
        edges: list[dict] = []
        seen: set = set()

        for row in rows:
            for key in ["player", "team", "season"]:
                item = row.get(key)
                if item and item.get("id") not in seen:
                    seen.add(item["id"])
                    nodes.append(item)
            for lst in ["appearances", "matches", "gameweeks"]:
                for item in (row.get(lst) or []):
                    if item and item.get("id") not in seen:
                        seen.add(item["id"])
                        nodes.append(item)

        # Build edges
        player_id = rows[0]["player"]["id"]
        team_id = rows[0]["team"]["id"]
        season_id = rows[0]["season"]["id"]
        edges.append({"source": player_id, "target": team_id, "type": "PLAYS_FOR"})
        edges.append({"source": team_id, "target": season_id, "type": "IN_SEASON"})

        app_edges = _query_graph(store, """
            MATCH (p:Player {web_name: $web_name})-[:HAD_APPEARANCE]->(a)-[:IN_MATCH]->(m)-[:PART_OF]->(g)
            RETURN elementId(p) AS p, elementId(a) AS a, elementId(m) AS m, elementId(g) AS g
        """, {"web_name": web_name})
        for e in app_edges:
            edges.append({"source": e["p"], "target": e["a"], "type": "HAD_APPEARANCE"})
            edges.append({"source": e["a"], "target": e["m"], "type": "IN_MATCH"})
            edges.append({"source": e["m"], "target": e["g"], "type": "PART_OF"})

        return {"nodes": nodes, "edges": edges}
    finally:
        store.close()


@app.get("/api/graph/match/{match_id}")
def graph_match(match_id: str) -> dict[str, Any]:
    """Subgraph for a single match."""
    store = _get_neo4j()
    try:
        rows = _query_graph(store, """
            MATCH (m:Match {match_id: $match_id})-[:PART_OF]->(g:Gameweek)
            OPTIONAL MATCH (t:Team)-[:HOME_TEAM|AWAY_TEAM]->(m)
            OPTIONAL MATCH (a:PlayerAppearance)-[:IN_MATCH]->(m)
            OPTIONAL MATCH (p:Player)-[:HAD_APPEARANCE]->(a)
            RETURN
              {id: elementId(m), name: m.match_id, type: 'Match',
               home: m.home_team_name, away: m.away_team_name,
               home_score: m.home_score, away_score: m.away_score, date: m.date} AS match,
              {id: elementId(g), name: toString(g.number), type: 'Gameweek', number: g.number} AS gameweek,
              collect(DISTINCT {id: elementId(t), name: t.name, type: 'Team'}) AS teams,
              collect(DISTINCT {id: elementId(a), name: a.name, type: 'PlayerAppearance',
                goals: a.goals_scored, assists: a.assists, points: a.total_points, minutes: a.minutes}) AS appearances,
              collect(DISTINCT {id: elementId(p), name: p.web_name, type: 'Player',
                position: p.position}) AS players
        """, {"match_id": match_id})

        if not rows:
            raise HTTPException(404, f"Match '{match_id}' not found")

        nodes: list[dict] = []
        edges: list[dict] = []
        seen: set = set()

        for row in rows:
            for key in ["match", "gameweek"]:
                item = row.get(key)
                if item and item.get("id") is not None and item["id"] not in seen:
                    seen.add(item["id"])
                    nodes.append(item)
            for lst in ["teams", "appearances", "players"]:
                for item in (row.get(lst) or []):
                    if item and item.get("id") is not None and item["id"] not in seen:
                        seen.add(item["id"])
                        nodes.append(item)

        edge_rows = _query_graph(store, """
            MATCH (m:Match {match_id: $match_id})-[:PART_OF]->(g)
            RETURN elementId(m) AS source, elementId(g) AS target, 'PART_OF' AS type
            UNION
            MATCH (t:Team)-[:HOME_TEAM]->(m:Match {match_id: $match_id})
            RETURN elementId(t) AS source, elementId(m) AS target, 'HOME_TEAM' AS type
            UNION
            MATCH (t:Team)-[:AWAY_TEAM]->(m:Match {match_id: $match_id})
            RETURN elementId(t) AS source, elementId(m) AS target, 'AWAY_TEAM' AS type
            UNION
            MATCH (p:Player)-[:HAD_APPEARANCE]->(a)-[:IN_MATCH]->(m:Match {match_id: $match_id})
            RETURN elementId(p) AS source, elementId(a) AS target, 'HAD_APPEARANCE' AS type
            UNION
            MATCH (a:PlayerAppearance)-[:IN_MATCH]->(m:Match {match_id: $match_id})
            RETURN elementId(a) AS source, elementId(m) AS target, 'IN_MATCH' AS type
        """, {"match_id": match_id})
        node_ids = {n["id"] for n in nodes}
        edges = [{"source": e["source"], "target": e["target"], "type": e["type"]}
                 for e in edge_rows if e["source"] in node_ids and e["target"] in node_ids]

        return {"nodes": nodes, "edges": edges}
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Stats and matches endpoints
# ---------------------------------------------------------------------------

@app.get("/api/stats/top-players")
def top_players(
    team: str = Query(default="", description="Team name filter"),
    stat: str = Query(default="goals_scored", description="Stat property to sort by"),
    limit: int = Query(default=10, ge=1, le=50),
) -> list[dict[str, Any]]:
    """Rank verified appearance metrics; transferred players use only requested clubs."""
    from src.analysis.deductions import AnalysisInputError, analyze
    from src.clean.corpus_quality import load_verified_corpus
    if stat not in ALLOWED_STATS:
        raise HTTPException(422, f"Unavailable metric. Supported: {sorted(ALLOWED_STATS)}")
    try:
        result = analyze(settings.data_dir, settings.season, "player_rankings",
                         teams=[team] if team else None, metric=ALLOWED_STATS[stat], limit=limit,
                         min_minutes=450 if stat.endswith("per90") else 0)
        players = {row["id"]: row for row in
                   load_verified_corpus(settings.data_dir, settings.season)["players"]}
        return [{"web_name": players[row["player_id"]]["web_name"],
                 "position": players[row["player_id"]]["position"], "value": row["value"],
                 "team": ", ".join(row["teams"]), "season": settings.season,
                 "metric_definition": result["definition"], "minutes": row["minutes"],
                 "appearances": row["appearances"], "player_id": row["player_id"],
                 "minimum_minutes": result["minimum_minutes"]}
                for row in result["rows"]]
    except AnalysisInputError as exc:
        raise HTTPException(422, str(exc)) from exc
    except (OSError, ValueError, KeyError) as exc:
        raise HTTPException(503, "Verified evidence is unavailable; run the MVP pipeline") from exc


@app.get("/api/matches")
def get_matches() -> list[dict[str, Any]]:
    """Return all 74 matches with scores and gameweek."""
    store = _get_neo4j()
    try:
        rows = _query_graph(store, """
            MATCH (m:Match)-[:PART_OF]->(g:Gameweek)
            RETURN m.match_id AS id,
                   m.date AS date,
                   m.home_team_name AS home_team,
                   m.away_team_name AS away_team,
                   m.home_score AS home_score,
                   m.away_score AS away_score,
                   g.number AS gameweek
            ORDER BY g.number ASC, m.date ASC
        """)
        return rows
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Verified local analysis endpoints
# ---------------------------------------------------------------------------

class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=4096)


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4096)
    history: list[ChatMessage] = Field(default_factory=list, max_length=8)


class AnalysisRequest(BaseModel):
    operation: Literal["team_stats", "form", "home_away", "player_rankings"]
    teams: list[str] = Field(default_factory=list, max_length=20)
    metric: Literal["goals", "assists", "minutes", "goals_per90", "assists_per90"] = "goals"
    min_minutes: int = Field(default=0, ge=0, le=10000)
    limit: int = Field(default=10, ge=1, le=100)


@app.post("/api/analysis")
def analysis(req: AnalysisRequest) -> dict[str, Any]:
    from src.analysis.deductions import AnalysisInputError, analyze
    try:
        return analyze(settings.data_dir, settings.season, **req.model_dump())
    except AnalysisInputError as exc:
        raise HTTPException(422, str(exc)) from exc
    except (OSError, ValueError, KeyError) as exc:
        raise HTTPException(503, "Verified evidence is unavailable; run the MVP pipeline") from exc


@app.post("/api/chat")
def chat(req: ChatRequest) -> dict[str, Any]:
    """Local deterministic answers; no provider credentials or billed generation."""
    from src.analysis.deductions import answer
    if not req.message.strip():
        raise HTTPException(422, "Message must contain text")
    try:
        return answer(settings.data_dir, settings.season, req.message)
    except (OSError, ValueError, KeyError) as exc:
        raise HTTPException(503, "Verified evidence is unavailable; run the MVP pipeline") from exc


@app.get("/api/evidence/search")
def evidence_search(query: str = Query(min_length=1, max_length=4096),
                    team: str = "", limit: int = Query(default=5, ge=1, le=20)):
    from src.store.mvp_index import search
    if not query.strip():
        raise HTTPException(422, "Query must contain text")
    try:
        return {"season": settings.season,
                "results": search(settings.data_dir, settings.season, query, team=team, limit=limit)}
    except (OSError, ValueError, KeyError) as exc:
        raise HTTPException(503, "Verified text index is unavailable; reload the MVP stores") from exc


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
