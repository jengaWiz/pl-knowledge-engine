"""Frozen temporal protocol: validation chooses a window; test never tunes it."""

from config.season import validate_season_dates
from src.corpus.contracts import sha256
from src.corpus.statistical_text import encoded
from src.prediction.baseline import (
    PRIOR_MATCHES,
    VERSION,
    WINDOWS,
    forecast,
    metrics,
    validate_matches,
)

TRAIN = "2022-23"
VALIDATION = "2023-24"
TEST = "2024-25"
PROTOCOL = "historical-hda-walk-forward-v1"


def evaluate(matches):
    validate_matches(matches)
    for match in matches:
        validate_season_dates([match["date"]], match["season"])
    seasons = {
        season: [m for m in matches if m["season"] == season]
        for season in (TRAIN, VALIDATION, TEST)
    }
    if any(not rows for rows in seasons.values()) or len(matches) != sum(
        map(len, seasons.values())
    ):
        raise ValueError("Evaluation requires exactly the frozen train/validation/test seasons")
    if not (
        max(m["date"] for m in seasons[TRAIN]) < min(m["date"] for m in seasons[VALIDATION])
        and max(m["date"] for m in seasons[VALIDATION]) < min(m["date"] for m in seasons[TEST])
    ):
        raise ValueError("Temporal season partitions overlap")

    def score(season, window):
        # Filter internally as well as inside forecast: validation cannot inspect test scores.
        history = [m for m in matches if m["date"] < max(r["date"] for r in seasons[season])]
        predictions = []
        for m in sorted(seasons[season], key=lambda m: (m["date"], m["id"])):
            result = forecast(history, m, window=window)
            outcome = (
                "home"
                if m["home_score"] > m["away_score"]
                else ("away" if m["away_score"] > m["home_score"] else "draw")
            )
            predictions.append(
                {"match_id": m["id"], "date": m["date"], "outcome": outcome, **result}
            )
        return predictions

    candidates = []
    for window in WINDOWS:
        candidates.append({"window": window, **metrics(score(VALIDATION, window))})
    chosen = min(candidates, key=lambda c: (c["log_loss"], c["window"]))["window"]
    test_predictions = score(TEST, chosen)
    league_predictions = score(TEST, None)
    uniform = [
        {"outcome": p["outcome"], "probabilities": dict.fromkeys(("home", "draw", "away"), 1 / 3)}
        for p in test_predictions
    ]
    return {
        "valid": True,
        "schema": PROTOCOL,
        "model_version": VERSION,
        "configuration": {
            "candidate_windows": list(WINDOWS),
            "prior_matches": PRIOR_MATCHES,
            "goal_rate_bounds": [0.1, 8.0],
            "poisson_max_goals": 60,
            "selection_metric": "validation_log_loss",
            "selection_tie_break": "smallest_window",
            "accuracy_tie_break": "home_then_draw_then_away",
        },
        "dataset_sha256": sha256(encoded(sorted(matches, key=lambda m: m["id"]))),
        "split": {"train": TRAIN, "validation": VALIDATION, "test": TEST},
        "protocol": "Walk-forward by date; earlier held-out results update later features. "
        "All same-day results excluded. Window selected on validation log loss only.",
        "limitations": [
            "Retrospective pinned results; source publication timestamps are unavailable.",
            "Not a reconstruction of what a live observer knew at kickoff.",
            "Independent Poisson baseline; no injuries, lineups, odds or tactical features.",
            "One held-out season; no calibrated-uncertainty or profitability claim.",
            "Final CSV revisions may contain corrections made after the fixture date.",
        ],
        "validation_candidates": candidates,
        "selected_window": chosen,
        "test_metrics": {
            "rolling_poisson": metrics(test_predictions),
            "league_poisson": metrics(league_predictions),
            "uniform": metrics(uniform),
        },
        "test_predictions": test_predictions,
        "league_test_predictions": league_predictions,
    }
