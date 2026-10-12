import copy

import pytest
from fastapi.testclient import TestClient

from backend import prediction
from backend.main import app
from src.prediction import service


def dataset():
    rows = []
    for year in (2022, 2023, 2024, 2025):
        for i in range(3):
            rows.append(
                {
                    "id": f"{year}:{i}",
                    "season": f"{year}-{(year + 1) % 100:02d}",
                    "date": f"{year}-08-{i + 1:02}",
                    "home_team": "Liverpool",
                    "away_team": "Aston Villa",
                    "home_score": i + 1,
                    "away_score": 1,
                    "finished": True,
                    "source": {
                        "row": i + 2,
                        "url": "https://example.org/results.csv",
                        "revision": "pinned",
                    },
                }
            )
    return rows


@pytest.fixture
def ready(monkeypatch):
    rows = dataset()
    monkeypatch.setattr(service, "load_results", lambda output: (copy.deepcopy(rows), []))
    monkeypatch.setattr(
        service, "evaluate", lambda rows: {"selected_window": 20, "test_metrics": {}}
    )
    monkeypatch.setattr(
        service, "evaluate_all", lambda rows, sources: {"selected_window": 20, "seasons": {}}
    )
    return rows, TestClient(app)


def test_catalogue_is_scoped_and_omits_results(ready):
    _, client = ready
    result = client.get("/api/prediction/status")
    assert result.status_code == 200
    data = result.json()
    assert {row["season"] for row in data["fixtures"]} == {"2024-25", "2025-26"}
    assert all("home_score" not in row for row in data["fixtures"])
    assert data["mode"] == "historical_replay"


def test_prediction_excludes_target_same_day_and_future_evidence(ready):
    rows, client = ready
    first = client.post("/api/analyst/prediction", json={"match_id": "2025:1"}).json()
    assert sum(first["probabilities"].values()) == pytest.approx(1)
    assert "home_score" not in first["fixture"]
    assert all(row["date"] < "2025-08-02" for row in first["evidence"])
    assert all(row["id"] != "2025:1" for row in first["evidence"])
    for row in rows:
        if row["date"] >= "2025-08-02":
            row.update(home_score=99, away_score=0)
    second = client.post("/api/analyst/prediction", json={"match_id": "2025:1"}).json()
    for key in ("probabilities", "expected_goals", "evidence", "explanation", "history_sha256"):
        assert first[key] == second[key]
    assert len(first["evidence"]) <= 40


@pytest.mark.parametrize(
    "body",
    [
        {"match_id": " "},
        {"match_id": "x" * 201},
        {},
        {"match_id": "2025:1", "window": 5},
        {"home_team": "Liverpool", "date": "2028-01-01"},
    ],
)
def test_invalid_or_future_freeform_requests_rejected(ready, body):
    assert ready[1].post("/api/analyst/prediction", json=body).status_code == 422


def test_unknown_and_wrong_season_fixture_rejected(ready):
    for identifier in ("missing", "2022:0"):
        assert (
            ready[1].post("/api/analyst/prediction", json={"match_id": identifier}).status_code
            == 404
        )


def test_unavailable_hides_internal_details_and_busy_is_retryable(ready, monkeypatch):
    def broken(*args):
        raise ValueError("private-path-provider-secret")

    monkeypatch.setattr(service, "load_results", broken)
    client = ready[1]
    for method, url, options in (
        (client.get, "/api/prediction/status", {}),
        (client.post, "/api/analyst/prediction", {"json": {"match_id": "x"}}),
    ):
        response = method(url, **options)
        assert response.status_code == 503 and "private-path" not in response.text
    with prediction.exclusive():
        response = client.get("/api/prediction/status")
        assert response.status_code == 429 and response.headers["Retry-After"] == "2"


def test_source_change_during_forecast_rejected(ready, monkeypatch):
    rows, _ = ready
    calls = 0

    def changing(output):
        nonlocal calls
        calls += 1
        result = copy.deepcopy(rows)
        if calls > 1:
            result[0]["home_score"] += 1
        return result, []

    monkeypatch.setattr(service, "load_results", changing)
    with pytest.raises(ValueError, match="changed during"):
        service.predict(None, "2025:1")


def test_two_season_evaluation_retains_frozen_selection_and_future_isolation():
    from src.prediction.workflow_evaluation import evaluate_all

    rows = dataset()
    first = evaluate_all(rows, [])
    rows[-1]["home_score"] = 99
    second = evaluate_all(rows, [])
    assert first["selected_window"] == second["selected_window"]
    assert first["seasons"]["2024-25"] == second["seasons"]["2024-25"]
    assert (
        first["seasons"]["2025-26"]["predictions"][0]
        == second["seasons"]["2025-26"]["predictions"][0]
    )
    assert set(first["seasons"]) == {"2024-25", "2025-26"}
