"""Optional evidence API: independent readiness, explicit seasons and bounded work."""

from contextlib import contextmanager
from pathlib import Path
from threading import BoundedSemaphore

from chromadb.utils.embedding_functions import ONNXMiniLM_L6_V2
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from filelock import Timeout
from neo4j.exceptions import DriverError, Neo4jError
from pydantic import BaseModel, ConfigDict, Field, field_validator

from config.season import season_bounds
from config.settings import settings
from src.retrieval.hybrid import retrieve
from src.store import evidence_index
from src.store.evidence_graph import prepare_graph
from src.store.evidence_queries import read

router = APIRouter(prefix="/api/evidence", tags=["Structured evidence"])
WORK = BoundedSemaphore(1)
UNAVAILABLE = (
    "Structured evidence is unavailable. Verify the corpus, graph, index and cached model."
)
ERRORS = (OSError, ValueError, KeyError, RuntimeError, Timeout, DriverError, Neo4jError)


def model_cached():
    path = Path(ONNXMiniLM_L6_V2.DOWNLOAD_PATH) / ONNXMiniLM_L6_V2.EXTRACTED_FOLDER_NAME
    required = (
        "config.json",
        "model.onnx",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "tokenizer.json",
        "vocab.txt",
    )
    return all((path / name).is_file() for name in required)


@contextmanager
def exclusive_work():
    # Integrity verification is expensive and store locks are deliberately nonblocking.
    if not WORK.acquire(blocking=False):
        raise HTTPException(
            429, "Evidence verification is busy. Retry shortly.", headers={"Retry-After": "2"}
        )
    try:
        yield
    finally:
        WORK.release()


class EvidenceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(min_length=1, max_length=4000)
    evidence_season: str
    limit: int = Field(default=5, ge=1, le=20, strict=True)

    @field_validator("query")
    @classmethod
    def require_text(cls, value):
        if not value.strip():
            raise ValueError("Query must contain text")
        return value

    @field_validator("evidence_season")
    @classmethod
    def require_season(cls, value):
        season_bounds(value)
        return value


def readiness():
    report = {
        "status": "not_ready",
        "primary_season": settings.season,
        "checks": {"corpus": False, "graph": False, "index": False, "model": False},
        "seasons": [],
    }
    try:
        settings.require_credentials("neo4j_password")
        plan = prepare_graph(settings.data_dir, settings.season)
        report["checks"]["corpus"] = True
        fixture = next(
            row["props"] for row in plan["nodes"] if row["props"].get("entity_type") == "Match"
        )
        hits = read(
            settings.data_dir,
            settings.season,
            settings.neo4j_uri,
            settings.neo4j_user,
            settings.neo4j_password,
            evidence_season=fixture["season"],
            entity_id=fixture["canonical_id"],
            limit=1,
        )
        if len(hits) != 1:
            raise ValueError("Graph readiness fixture is missing")
        report["checks"]["graph"] = True
        index = evidence_index.status(settings.data_dir, settings.season)
        report["checks"]["index"] = True
        if not model_cached():
            raise ValueError("Local model cache is unavailable")
        report["checks"]["model"] = True
        if prepare_graph(settings.data_dir, settings.season)["dataset_id"] != plan["dataset_id"]:
            raise ValueError("Evidence changed during readiness")
        report.update(
            status="ready",
            seasons=sorted(index["records_by_season"]),
            index=index,
            graph_dataset_id=plan["dataset_id"],
            graph_nodes=sum(plan["counts"].values()),
        )
    except ERRORS:
        report["detail"] = UNAVAILABLE
    return report


@router.get("/status")
def evidence_status():
    with exclusive_work():
        report = readiness()
        return JSONResponse(report, status_code=200 if report["status"] == "ready" else 503)


@router.post("/retrieve")
def evidence_retrieve(req: EvidenceRequest):
    with exclusive_work():
        try:
            settings.require_credentials("neo4j_password")
            if not model_cached():
                raise ValueError("Model must be cached before serving requests")
            return retrieve(
                settings.data_dir,
                settings.season,
                req.query,
                settings.neo4j_uri,
                settings.neo4j_user,
                settings.neo4j_password,
                evidence_season=req.evidence_season,
                limit=req.limit,
            )
        except ERRORS as exc:
            raise HTTPException(503, UNAVAILABLE) from exc
