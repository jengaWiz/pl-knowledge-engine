import copy
import math

import pytest

from src.prediction.baseline import forecast, metrics, outcome_probabilities, validate_matches
from src.prediction.evaluation import evaluate


def match(identifier, day, home="A", away="B", scores=(2, 1), season="2022-23"):
    return {
        "id": identifier,
        "date": day,
        "season": season,
        "home_team": home,
        "away_team": away,
        "home_score": scores[0],
        "away_score": scores[1],
        "finished": True,
    }


def test_poisson_matches_independent_known_rates_and_symmetry():
    p = outcome_probabilities(1, 1)
    # P(draw) = exp(-2) * sum(1/(k!)**2); independently calculated reference.
    draw = math.exp(-2) * sum(1 / math.factorial(k) ** 2 for k in range(20))
    assert p["draw"] == pytest.approx(draw)
    assert p["home"] == pytest.approx((1 - draw) / 2)
    assert p["away"] == pytest.approx(p["home"])
    unequal = outcome_probabilities(2, 0.5)
    swapped = outcome_probabilities(0.5, 2)
    assert unequal["home"] > unequal["away"]
    assert unequal["home"] == pytest.approx(swapped["away"])
    assert sum(outcome_probabilities(8, 0.1).values()) == pytest.approx(1)


@pytest.mark.parametrize("rate", [0, -1, 9, float("inf"), float("nan")])
def test_invalid_goal_rates(rate):
    with pytest.raises(ValueError):
        outcome_probabilities(rate, 1)


def test_same_day_future_and_target_outcomes_never_enter_features():
    target = match("target", "2023-09-02", season="2023-24")
    past = [match("past", "2023-08-01", scores=(3, 1), season="2023-24")]
    ignored = [
        target,
        match("same", target["date"], scores=(99, 99)),
        match("future", "2024-01-01", scores=(99, 0)),
    ]
    expected = forecast(past, target, window=5)
    assert forecast(past + ignored, target, window=5) == expected
    changed = copy.deepcopy(target)
    changed.update(home_score=99, away_score=99, result="D", home_shots=1000)
    assert forecast(past + ignored, changed, window=5) == expected
    assert expected["latest_history_date"] < expected["feature_cutoff_exclusive"]
    assert expected["team_history_ids"] == {"A": ["past"], "B": ["past"]}


def test_rolling_features_use_only_window_and_smooth_unseen_clubs():
    rows = [match(str(i), f"2023-08-{i + 1:02}", scores=(i, 1)) for i in range(8)]
    target = match("t", "2023-09-01", home="A", away="Promoted")
    result = forecast(rows, target, window=5)
    assert result["team_history_ids"]["A"] == [str(i) for i in range(3, 8)]
    assert result["team_history_ids"]["Promoted"] == []
    assert all(0 < p < 1 for p in result["probabilities"].values())
    assert forecast(list(reversed(rows)), target, window=5) == result


def test_league_baseline_uses_prior_home_and_away_rates():
    result = forecast(
        [match("a", "2023-01-01", scores=(4, 2)), match("b", "2023-01-02", scores=(2, 0))],
        match("t", "2023-02-01"),
    )
    assert result["expected_goals"] == {"home": 3, "away": 1}
    assert result["probabilities"] == outcome_probabilities(3, 1)


def test_metrics_have_independent_reference_and_reject_invalid_probabilities():
    p = {"home": 0.5, "draw": 0.25, "away": 0.25}
    result = metrics([{"outcome": "home", "probabilities": p}])
    assert result == {"matches": 1, "log_loss": -math.log(0.5), "brier": 0.375, "accuracy": 1}
    for scores in ({**p, "away": 0.5}, {**p, "away": float("nan")}, {"home": 1}):
        with pytest.raises(ValueError):
            metrics([{"outcome": "home", "probabilities": scores}])
    with pytest.raises(ValueError):
        metrics([])


@pytest.mark.parametrize(
    "changes",
    [
        {"home_score": True},
        {"away_score": -1},
        {"finished": False},
        {"home_team": "B"},
        {"date": "2023-2-01"},
        {"season": ""},
        {"id": ""},
    ],
)
def test_invalid_matches(changes):
    with pytest.raises(ValueError):
        validate_matches([{**match("a", "2023-01-01"), **changes}])


def test_duplicate_no_history_and_invalid_windows_rejected():
    row = match("a", "2023-01-01")
    with pytest.raises(ValueError):
        validate_matches([row, row])
    with pytest.raises(ValueError):
        forecast([row], row)
    with pytest.raises(ValueError):
        forecast([row], match("b", "2023-02-01"), window=True)


def temporal_rows():
    return [
        match("train", "2023-05-01"),
        match("v1", "2023-08-01", season="2023-24"),
        match("v2", "2024-05-01", scores=(0, 2), season="2023-24"),
        match("t1", "2024-08-01", season="2024-25"),
        match("t2", "2024-08-02", scores=(1, 1), season="2024-25"),
    ]


def test_test_outcomes_cannot_tune_window_or_affect_earlier_predictions():
    rows = temporal_rows()
    first = evaluate(rows)
    changed = copy.deepcopy(rows)
    changed[-1].update(home_score=50, away_score=0)
    second = evaluate(changed)
    assert first["validation_candidates"] == second["validation_candidates"]
    assert first["selected_window"] == second["selected_window"]
    assert first["test_predictions"][0] == second["test_predictions"][0]
    assert (
        first["test_predictions"][1]["probabilities"]
        == second["test_predictions"][1]["probabilities"]
    )
    assert first["test_metrics"] != second["test_metrics"]
    assert first["dataset_sha256"] != second["dataset_sha256"]
    assert first == evaluate(list(reversed(rows)))


def test_walk_forward_updates_only_after_prior_test_day():
    rows = temporal_rows()
    first = evaluate(rows)
    rows[-2].update(home_score=10, away_score=0)
    second = evaluate(rows)
    assert (
        first["test_predictions"][0]["probabilities"]
        == second["test_predictions"][0]["probabilities"]
    )
    assert (
        first["test_predictions"][1]["probabilities"]
        != second["test_predictions"][1]["probabilities"]
    )


def test_partition_contract_rejects_missing_extra_or_overlapping_seasons():
    rows = temporal_rows()
    for candidate in (
        rows[:3],
        rows + [match("extra", "2025-08-01", season="2025-26")],
        [{**m, "date": "2023-01-01"} if m["id"] == "v1" else m for m in rows],
    ):
        with pytest.raises(ValueError):
            evaluate(candidate)
