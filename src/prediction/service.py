"""Historical replay forecasts with bounded, strictly earlier result explanations."""

from src.corpus.contracts import sha256
from src.corpus.statistical_text import encoded
from src.prediction.baseline import VERSION, forecast
from src.prediction.evaluation import evaluate
from src.prediction.sources import load_results
from src.prediction.workflow_evaluation import evaluate_all

REPLAY_SEASONS = ("2024-25", "2025-26")
LIMITATIONS = [
    "Historical replay, not a live upcoming-match forecast.",
    "Only results strictly before the fixture date enter features; same-day results are excluded.",
    "Source publication times are unavailable; later CSV corrections may exist.",
    "No injury, lineup, odds or tactical evidence is used. Probabilities are not guarantees.",
]


class UnknownFixtureError(ValueError):
    pass


def verified(output):
    rows, sources = load_results(output)
    historical = [
        {k: v for k, v in row.items() if k != "source"}
        for row in rows
        if row["season"] != "2025-26"
    ]
    evaluation = evaluate(historical)
    if evaluation["selected_window"] != 20:
        raise ValueError("Frozen model selection changed")
    return rows, sources, evaluation


def catalogue(output):
    rows, sources = load_results(output)
    evaluation = evaluate_all(rows, sources)
    if evaluation["selected_window"] != 20:
        raise ValueError("Frozen model selection changed")
    return {
        "status": "ready",
        "mode": "historical_replay",
        "model_version": VERSION,
        "window": 20,
        "selection_season": "2023-24",
        "limitations": LIMITATIONS,
        "evaluations": {
            season: {
                key: value
                for key, value in report.items()
                if key not in ("predictions", "league_predictions")
            }
            for season, report in evaluation["seasons"].items()
        },
        "evaluation_season": "2024-25",
        "fixtures": [
            {key: row[key] for key in ("id", "season", "date", "home_team", "away_team")}
            for row in rows
            if row["season"] in REPLAY_SEASONS
        ],
    }


def predict(output, match_id):
    rows, sources, _ = verified(output)
    matches = [row for row in rows if row["id"] == match_id and row["season"] in REPLAY_SEASONS]
    if len(matches) != 1:
        raise UnknownFixtureError(
            "Select a supported historical fixture from the prediction catalogue"
        )
    fixture = matches[0]
    result = forecast(rows, fixture, window=20)
    history = [row for row in rows if row["date"] < fixture["date"] and row["id"] != match_id]
    recent_ids = {identifier for ids in result["team_history_ids"].values() for identifier in ids}
    evidence = [
        {
            key: row[key]
            for key in (
                "id",
                "date",
                "home_team",
                "away_team",
                "home_score",
                "away_score",
                "source",
            )
        }
        for row in history
        if row["id"] in recent_ids
    ]
    if any(row["date"] >= fixture["date"] for row in evidence):
        raise ValueError("Explanation crossed its exclusive cutoff")
    # Sources are immutable/pinned but detect a replacement during computation too.
    check, _ = load_results(output)
    if encoded(rows) != encoded(check):
        raise ValueError("Prediction sources changed during the request")
    team_counts = {team: len(ids) for team, ids in result["team_history_ids"].items()}
    return {
        "mode": "historical_replay",
        "model_version": VERSION,
        "selection_season": "2023-24",
        "fixture": {
            key: fixture[key] for key in ("id", "season", "date", "home_team", "away_team")
        },
        **result,
        "limitations": LIMITATIONS,
        "evidence": evidence,
        "history_sha256": sha256(encoded(history)),
        "dataset_sha256": sha256(encoded(rows)),
        "source_contracts": [s for s in sources if s["season"] in {m["season"] for m in history}],
        "explanation": f"Uses {len(history)} earlier league results and up to 20 recent "
        f"results per club ({fixture['home_team']}: "
        f"{team_counts[fixture['home_team']]}; {fixture['away_team']}: "
        f"{team_counts[fixture['away_team']]}). Recent scoring and conceding "
        "rates are smoothed toward league averages. The statistical model "
        "supplies all probabilities; this explanation adds no tactical claims.",
        "evidence_scope": "Cards show the union of recent team results (at most 40). "
        "Expanding league averages use all earlier results in the history hash. "
        "No unrestricted graph/vector retrieval or generated explanation is used.",
    }
