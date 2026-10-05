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

@app.get("/api/graph/overview")
def graph_overview():
    from backend.graph import read_graph
    return read_graph("overview")


@app.get("/api/graph/player/{web_name}")
def graph_player(web_name: str):
    from backend.graph import read_graph
    return read_graph("player",web_name)


@app.get("/api/graph/match/{match_id}")
def graph_match(match_id: str):
    from backend.graph import read_graph
    return read_graph("match",match_id)


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
    from backend.graph import fixtures
    return fixtures()


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


@app.get("/api/readiness")
def ready():
    from fastapi.responses import JSONResponse
    from backend.readiness import readiness

    report = readiness()
    return JSONResponse(report, status_code=200 if report["status"] == "ready" else 503)


# Register static assets after API routes; Docker serves the built dashboard here.
if Path("frontend/dist").is_dir():
    from fastapi.staticfiles import StaticFiles

    app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="dashboard")
