"""Frozen two-season retrospective prediction evaluation and diagnostics."""

from src.corpus.contracts import sha256
from src.corpus.statistical_text import encoded
from src.prediction.baseline import forecast, metrics
from src.prediction.diagnostics import calibration, intervals
from src.prediction.evaluation import evaluate


def evaluate_all(rows, sources):
    original = evaluate(
        [{k: v for k, v in m.items() if k != "source"} for m in rows if m["season"] != "2025-26"]
    )
    window = original["selected_window"]
    seasons = {}
    for season in ("2024-25", "2025-26"):
        own, league = [], []
        for match in rows:
            if match["season"] != season:
                continue
            target = (
                "home"
                if match["home_score"] > match["away_score"]
                else ("away" if match["home_score"] < match["away_score"] else "draw")
            )
            for predictions, w in ((own, window), (league, None)):
                predictions.append(
                    {
                        "match_id": match["id"],
                        "date": match["date"],
                        "outcome": target,
                        **forecast(rows, match, window=w),
                    }
                )
        seasons[season] = {
            "rolling": metrics(own),
            "league": metrics(league),
            "calibration": calibration(own),
            "uncertainty": intervals(own, league),
            "predictions": own,
            "league_predictions": league,
        }
    return {
        "valid": True,
        "schema": "prediction-workflow-evaluation-v1",
        "dataset_sha256": sha256(encoded(rows)),
        "selected_window": window,
        "selection_season": "2023-24",
        "sources": sources,
        "seasons": seasons,
        "limitations": [
            item for item in original["limitations"] if not item.startswith("One held-out")
        ]
        + [
            "2025-26 is an additional retrospective season, not a prospective preregistered trial.",
            "Calibration bins describe errors; probabilities were not recalibrated.",
        ],
    }
