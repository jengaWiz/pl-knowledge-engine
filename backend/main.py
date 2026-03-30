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
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config.settings import settings
from src.store.neo4j_store import Neo4jStore
from src.store.chroma_store import ChromaStore
from src.utils.logger import get_logger

logger = get_logger(__name__)

ALLOWED_STATS = {
    "goals_scored", "assists", "clean_sheets", "minutes", "yellow_cards",
    "red_cards", "expected_goals", "expected_assists", "total_points",
    "form", "points_per_game", "starts", "influence", "creativity",
    "threat", "ict_index", "now_cost", "bonus", "bps",
}

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
    return Neo4jStore(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)


def _get_chroma() -> ChromaStore:
    return ChromaStore()


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
    """Return top players sorted by a stat property."""
    if stat not in ALLOWED_STATS:
        raise HTTPException(400, f"Invalid stat '{stat}'. Allowed: {sorted(ALLOWED_STATS)}")
    store = _get_neo4j()
    try:
        if team:
            rows = _query_graph(store, f"""
                MATCH (p:Player)-[:PLAYS_FOR]->(t:Team {{name: $team}})
                WHERE p.{stat} IS NOT NULL
                RETURN p.web_name AS web_name, p.position AS position,
                       p.{stat} AS value, t.name AS team
                ORDER BY p.{stat} DESC
                LIMIT $limit
            """, {"team": team, "limit": limit})
        else:
            rows = _query_graph(store, f"""
                MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
                WHERE p.{stat} IS NOT NULL
                RETURN p.web_name AS web_name, p.position AS position,
                       p.{stat} AS value, t.name AS team
                ORDER BY p.{stat} DESC
                LIMIT $limit
            """, {"limit": limit})
        return rows
    finally:
        store.close()


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
# RAG Chatbot endpoint
# ---------------------------------------------------------------------------

class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []


def _embed_query(text: str) -> list[float]:
    """Embed a query string using Gemini Embedding 2."""
    from google import genai
    client = genai.Client(api_key=settings.gemini_api_key)
    response = client.models.embed_content(
        model=settings.gemini_model,
        contents=text,
    )
    return response.embeddings[0].values


def _neo4j_context(store: Neo4jStore, message: str) -> str:
    """Extract structured Neo4j context based on message intent."""
    msg_lower = message.lower()
    context_parts: list[str] = []

    # Detect player mentions
    player_rows = _query_graph(store, """
        MATCH (p:Player) RETURN p.web_name AS name
    """)
    player_names = [r["name"] for r in player_rows if r["name"]]

    mentioned = [n for n in player_names if n.lower() in msg_lower]
    for name in mentioned[:3]:
        rows = _query_graph(store, """
            MATCH (p:Player {web_name: $name})-[:PLAYS_FOR]->(t:Team)
            RETURN p.web_name AS name, t.name AS team, p.goals_scored AS goals,
                   p.assists AS assists, p.total_points AS points,
                   p.expected_goals AS xg, p.form AS form, p.position AS position
        """, {"name": name})
        if rows:
            r = rows[0]
            context_parts.append(
                f"Player {r['name']} ({r['team']}, {r['position']}): "
                f"{r['goals']} goals, {r['assists']} assists, "
                f"{r['points']} FPL points, form {r['form']}, xG {r['xg']}"
            )

    # Top scorers query
    if any(w in msg_lower for w in ["top scorer", "most goals", "scored the most"]):
        rows = _query_graph(store, """
            MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
            WHERE p.goals_scored IS NOT NULL
            RETURN p.web_name AS name, t.name AS team, p.goals_scored AS goals
            ORDER BY p.goals_scored DESC LIMIT 10
        """)
        if rows:
            context_parts.append("Top scorers: " + ", ".join(
                f"{r['name']} ({r['team']}, {r['goals']} goals)" for r in rows
            ))

    # Top assisters
    if any(w in msg_lower for w in ["most assist", "top assist", "most creative"]):
        rows = _query_graph(store, """
            MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
            WHERE p.assists IS NOT NULL
            RETURN p.web_name AS name, t.name AS team, p.assists AS assists
            ORDER BY p.assists DESC LIMIT 10
        """)
        if rows:
            context_parts.append("Top assisters: " + ", ".join(
                f"{r['name']} ({r['team']}, {r['assists']})" for r in rows
            ))

    # Team comparison
    if any(w in msg_lower for w in ["villa", "aston villa"]) and any(w in msg_lower for w in ["liverpool", "reds"]):
        for team_name in ["Aston Villa", "Liverpool"]:
            rows = _query_graph(store, """
                MATCH (p:Player)-[:PLAYS_FOR]->(t:Team {name: $team})
                RETURN sum(p.goals_scored) AS total_goals,
                       sum(p.assists) AS total_assists,
                       sum(p.total_points) AS total_points
            """, {"team": team_name})
            if rows and rows[0]:
                r = rows[0]
                context_parts.append(
                    f"{team_name} season totals: {r['total_goals']} goals, "
                    f"{r['total_assists']} assists, {r['total_points']} FPL points"
                )

    # Recent form / last 5
    if any(w in msg_lower for w in ["form", "last 5", "recent"]):
        rows = _query_graph(store, """
            MATCH (m:Match)-[:PART_OF]->(g:Gameweek)
            RETURN m.home_team_name AS home, m.away_team_name AS away,
                   m.home_score AS hs, m.away_score AS as, g.number AS gw
            ORDER BY g.number DESC LIMIT 10
        """)
        if rows:
            context_parts.append("Recent matches: " + "; ".join(
                f"GW{r['gw']}: {r['home']} {r['hs']}-{r['as']} {r['away']}" for r in rows
            ))

    return "\n".join(context_parts) if context_parts else ""


@app.post("/api/chat")
async def chat(req: ChatRequest) -> dict[str, Any]:
    """RAG chatbot: semantic search + Neo4j context + Gemini generation."""
    from google import genai
    from google.genai import types

    store = _get_neo4j()
    chroma = _get_chroma()
    sources: list[dict] = []

    try:
        # 1. Embed query and search ChromaDB
        try:
            query_vec = _embed_query(req.message)
            chroma_results = chroma.search_text(query_vec, n_results=5)
            rag_context = "\n\n".join(
                r["document"] for r in chroma_results if r.get("document")
            )
            for r in chroma_results[:3]:
                meta = r.get("metadata", {})
                sources.append({
                    "type": meta.get("source_type", "stats"),
                    "summary": (r.get("document") or "")[:150],
                })
        except Exception as e:
            logger.warning("chroma search failed", error=str(e))
            rag_context = ""

        # 2. Neo4j structured context
        try:
            neo4j_ctx = _neo4j_context(store, req.message)
        except Exception as e:
            logger.warning("neo4j context failed", error=str(e))
            neo4j_ctx = ""

        # 3. Build system prompt
        system_prompt = (
            "You are an expert Premier League football analyst for Aston Villa and Liverpool FC "
            "in the 2025-26 season. Answer questions using the context below. "
            "Be specific, cite statistics, and be engaging.\n\n"
        )
        if neo4j_ctx:
            system_prompt += f"## Structured Data\n{neo4j_ctx}\n\n"
        if rag_context:
            system_prompt += f"## Relevant Podcast/Article Context\n{rag_context}\n\n"
        system_prompt += "If you don't have enough data to answer precisely, say so honestly."

        # 4. Build conversation for Gemini
        client = genai.Client(api_key=settings.gemini_api_key)
        contents = []

        for msg in req.history[-8:]:  # last 8 messages for context window
            contents.append(types.Content(
                role=msg.role if msg.role == "user" else "model",
                parts=[types.Part(text=msg.content)],
            ))
        contents.append(types.Content(
            role="user",
            parts=[types.Part(text=req.message)],
        ))

        response = client.models.generate_content(
            model=settings.gemini_text_model,
            config=types.GenerateContentConfig(system_instruction=system_prompt),
            contents=contents,
        )
        reply = response.text

    except Exception as exc:
        logger.error("chat endpoint error", error=str(exc))
        raise HTTPException(500, str(exc))
    finally:
        store.close()

    return {"reply": reply, "sources": sources}


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
