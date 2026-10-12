"""Optional, local statistical prediction workflow; independent of graph/provider readiness."""

from contextlib import contextmanager
from threading import BoundedSemaphore

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

from config.settings import settings
from src.prediction import service

router = APIRouter(tags=["Historical predictions"])
WORK = BoundedSemaphore(1)
UNAVAILABLE = "Prediction results are unavailable. Prepare the MVP and historical result sources."


@contextmanager
def exclusive():
    if not WORK.acquire(blocking=False):
        raise HTTPException(
            429, "Prediction verification is busy. Retry shortly.", headers={"Retry-After": "2"}
        )
    try:
        yield
    finally:
        WORK.release()


class PredictionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    match_id: str = Field(min_length=1, max_length=200)

    @field_validator("match_id")
    @classmethod
    def nonempty(cls, value):
        if not value.strip():
            raise ValueError("Fixture identity is required")
        return value


@router.get("/api/prediction/status")
def status():
    with exclusive():
        try:
            return service.catalogue(settings.data_dir)
        except (OSError, ValueError, KeyError, RuntimeError) as exc:
            raise HTTPException(503, UNAVAILABLE) from exc


@router.post("/api/analyst/prediction")
def prediction(request: PredictionRequest):
    with exclusive():
        try:
            return service.predict(settings.data_dir, request.match_id)
        except service.UnknownFixtureError as exc:
            raise HTTPException(404, str(exc)) from exc
        except (OSError, ValueError, KeyError, RuntimeError) as exc:
            raise HTTPException(503, UNAVAILABLE) from exc
