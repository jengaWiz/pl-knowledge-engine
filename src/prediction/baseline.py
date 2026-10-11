"""Transparent Poisson baselines using only completed results before a fixture date."""

import math
from datetime import date

VERSION = "prematch-poisson-v1"
WINDOWS = (5, 10, 20)
PRIOR_MATCHES = 10


def validate_matches(matches):
    """Reject malformed or duplicate outcomes before computing any features."""
    seen = set()
    for match in matches:
        if not isinstance(match.get("id"), str) or not match["id"] or match["id"] in seen:
            raise ValueError("Match identities must be nonempty and unique")
        seen.add(match["id"])
        if date.fromisoformat(match["date"]).isoformat() != match["date"]:
            raise ValueError("Match dates must be ISO calendar dates")
        if not match.get("season") or not match.get("home_team") or not match.get("away_team"):
            raise ValueError("Season and team identities are required")
        if match["home_team"] == match["away_team"] or match.get("finished") is not True:
            raise ValueError("Only completed fixtures between distinct teams are supported")
        for key in ("home_score", "away_score"):
            if type(match.get(key)) is not int or match[key] < 0:
                raise ValueError("Scores must be nonnegative integers")


def outcome_probabilities(home_rate, away_rate):
    """Independent Poisson goals; normalize negligible tails beyond 60 goals."""

    def distribution(rate):
        if not math.isfinite(rate) or not 0 < rate <= 8:
            raise ValueError("Goal rates must be finite and in (0, 8]")
        values = [math.exp(-rate)]
        for goals in range(1, 61):
            values.append(values[-1] * rate / goals)
        total = math.fsum(values)
        return [value / total for value in values]

    home, away = distribution(home_rate), distribution(away_rate)
    draw = math.fsum(h * a for h, a in zip(home, away, strict=True))
    win = math.fsum(h * math.fsum(away[:i]) for i, h in enumerate(home))
    loss = math.fsum(a * math.fsum(home[:i]) for i, a in enumerate(away))
    total = win + draw + loss
    return {"home": win / total, "draw": draw / total, "away": loss / total}


def forecast(history, fixture, *, window=None):
    """Exclude target/same-day/future results even if a caller passes the full corpus.

    Day resolution is deliberate: kickoff ordering is unavailable. Published-at
    timestamps are also unavailable, so this is retrospective result-date testing.
    """
    if window is not None and (type(window) is not int or window not in WINDOWS):
        raise ValueError("Unsupported rolling window")
    cutoff = date.fromisoformat(fixture["date"]).isoformat()
    home_team, away_team = fixture["home_team"], fixture["away_team"]
    if not home_team or not away_team or home_team == away_team:
        raise ValueError("Forecast requires two distinct teams")
    eligible = sorted(
        (m for m in history if m["date"] < cutoff and m["id"] != fixture["id"]),
        key=lambda m: (m["date"], m["id"]),
    )
    validate_matches(eligible)
    if not eligible:
        raise ValueError("No completed results precede the fixture date")
    n = len(eligible)
    home_mean = sum(m["home_score"] for m in eligible) / n
    away_mean = sum(m["away_score"] for m in eligible) / n
    league_mean = (home_mean + away_mean) / 2
    # A nonzero floor keeps degenerate/synthetic scoreless histories well-defined.
    home_rate, away_rate = max(0.1, home_mean), max(0.1, away_mean)
    league_mean = max(0.1, league_mean)
    refs = {}

    def strength(team):
        recent = [m for m in eligible if team in (m["home_team"], m["away_team"])][-window:]
        scored = conceded = 0
        for m in recent:
            is_home = m["home_team"] == team
            scored += m["home_score"] if is_home else m["away_score"]
            conceded += m["away_score"] if is_home else m["home_score"]
        denominator = len(recent) + PRIOR_MATCHES
        refs[team] = [m["id"] for m in recent]
        return (
            (scored + PRIOR_MATCHES * league_mean) / denominator / league_mean,
            (conceded + PRIOR_MATCHES * league_mean) / denominator / league_mean,
        )

    if window is not None:
        home_attack, home_defense = strength(home_team)
        away_attack, away_defense = strength(away_team)
        home_rate *= home_attack * away_defense
        away_rate *= away_attack * home_defense
    home_rate, away_rate = (min(8.0, max(0.1, value)) for value in (home_rate, away_rate))
    return {
        "model": "league-poisson" if window is None else "rolling-poisson",
        "window": window,
        "expected_goals": {"home": home_rate, "away": away_rate},
        "probabilities": outcome_probabilities(home_rate, away_rate),
        "feature_cutoff_exclusive": cutoff,
        "history_matches": n,
        "latest_history_date": eligible[-1]["date"],
        "team_history_ids": refs,
    }


def metrics(predictions):
    """Multiclass Brier is the sum over all three classes (range 0–2)."""
    if not predictions:
        raise ValueError("Cannot score an empty evaluation set")
    log_loss = brier = correct = 0
    for row in predictions:
        scores = row["probabilities"]
        target = row["outcome"]
        if set(scores) != {"home", "draw", "away"} or target not in scores:
            raise ValueError("Expected home/draw/away outcomes")
        if any(not math.isfinite(v) or not 0 <= v <= 1 for v in scores.values()):
            raise ValueError("Invalid probabilities")
        if not math.isclose(sum(scores.values()), 1, abs_tol=1e-9):
            raise ValueError("Probabilities must sum to one")
        log_loss -= math.log(max(scores[target], 1e-15))
        brier += sum((p - (kind == target)) ** 2 for kind, p in scores.items())
        correct += max(scores, key=scores.get) == target
    n = len(predictions)
    return {"matches": n, "log_loss": log_loss / n, "brier": brier / n, "accuracy": correct / n}
