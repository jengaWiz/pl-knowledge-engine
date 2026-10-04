"""Hand-calculated fixtures validate deductions and abstention behavior."""

import pytest

from src.analysis import deductions


@pytest.fixture
def corpus(monkeypatch):
    scores = [(2, 1), (0, 0), (1, 3), (4, 0), (2, 2), (0, 1)]
    matches = [
        {
            "id": f"m{i}",
            "date": f"2026-01-{i + 1:02}",
            "home_team": "Liverpool" if i % 2 == 0 else "Aston Villa",
            "away_team": "Aston Villa" if i % 2 == 0 else "Liverpool",
            "home_score": home,
            "away_score": away,
            "source_url": "https://example.org/matches",
            "source_sha256": "abc",
            "source_row": i + 2,
        }
        for i, (home, away) in enumerate(scores)
    ]
    players = [{"id": "p1", "name": "Player One"}, {"id": "p2", "name": "Player Two"}]
    for player in players:
        player["source"] = {"url": "https://example.org/players", "sha256": "xyz", "row": 2}
    appearances = [
        {
            "id": f"a{i}",
            "player_id": player,
            "date": "2026-01-01",
            "team": "Liverpool",
            "goals": goals,
            "assists": assists,
            "minutes": minutes,
            "source": {"url": "https://example.org/appearances", "sha256": "def", "row": i + 2},
        }
        for i, (player, goals, assists, minutes) in enumerate(
            [("p1", 2, 1, 180), ("p2", 1, None, 30)]
        )
    ]
    data = {"matches": matches, "players": players, "appearances": appearances}
    monkeypatch.setattr(deductions, "load_verified_corpus", lambda *args: data)
    return data


def test_team_result_totals_and_source_ids(corpus, tmp_path):
    result = deductions.analyze(tmp_path, "2025-26", "team_stats", teams=["Liverpool"])
    # Liverpool: 2-1, 0-0, 1-3, 0-4, 2-2, 1-0 => W D L L D W.
    row = result["rows"][0]
    assert (row["points"], row["goals_for"], row["goals_against"], row["goal_difference"]) == (
        8,
        6,
        10,
        -4,
    )
    assert (row["wins"], row["draws"], row["losses"]) == (2, 2, 2)
    assert result["sample_size"] == 6
    assert result["sources"][0]["record_ids"] == [f"m{i}" for i in range(6)]
    assert result["sources"][0]["source_rows"] == [2, 3, 4, 5, 6, 7]


def test_last_five_excludes_oldest_and_is_team_relative(corpus, tmp_path):
    result = deductions.analyze(tmp_path, "2025-26", "form", teams=["Liverpool"])
    assert result["rows"][0]["form"] == "DLLDW"
    assert result["rows"][0]["points"] == 5
    assert result["date_range"]["from"] == "2026-01-02"
    assert result["sample_size"] == 5


def test_home_away_splits(corpus, tmp_path):
    row = deductions.analyze(tmp_path, "2025-26", "home_away", teams=["Liverpool"])["rows"][0]
    assert row["home"]["points"] == 4 and row["away"]["points"] == 4
    assert row["home"]["goals_for"] == 5 and row["away"]["goals_for"] == 1
    assert row["home"]["matches"] == row["away"]["matches"] == 3


def test_per90_threshold_and_unknown_assists(corpus, tmp_path):
    rate = deductions.analyze(
        tmp_path, "2025-26", "player_rankings", metric="goals_per90", min_minutes=90
    )
    assert rate["rows"][0]["name"] == "Player One" and rate["rows"][0]["value"] == 1
    assert len(rate["rows"]) == 1 and rate["excluded_players"]["below_minimum_minutes"] == 1
    assists = deductions.analyze(tmp_path, "2025-26", "player_rankings", metric="assists")
    assert assists["rows"][0]["value"] == 1 and len(assists["rows"]) == 1
    assert assists["excluded_players"]["missing_metric"] == 1


@pytest.mark.parametrize(
    "message",
    [
        "Why did Liverpool lose?",
        "Will Villa win next year?",
        "Liverpool points 2024-25",
        "What do podcasts say?",
        "Which players cost the most?",
        "Liverpool FPL points",
        "Who had most clean sheets?",
        "Compare Watkins and Salah",
    ],
)
def test_unsupported_questions_do_not_invent_answers(corpus, tmp_path, message):
    result = deductions.answer(tmp_path, "2025-26", message)
    assert result["status"] == "insufficient_evidence" and result["sources"] == []


@pytest.mark.parametrize(
    "message,operation",
    [
        ("Liverpool points and goal difference", "team_stats"),
        ("How has Aston Villa performed in their last 5 games?", "form"),
        ("Compare Aston Villa and Liverpool home versus away", "home_away"),
        ("Who are Liverpool's top scorers this season?", "player_rankings"),
        ("Which players have most assists across both teams?", "player_rankings"),
    ],
)
def test_supported_questions_return_evidence(corpus, tmp_path, message, operation):
    result = deductions.answer(tmp_path, "2025-26", message)
    assert result["status"] == "ok" and result["operation"] == operation
    assert result["sources"] and "2025-26" in result["reply"]


def test_transferred_player_totals_are_club_scoped(corpus, tmp_path):
    source = corpus["appearances"][0]["source"]
    corpus["appearances"].append(
        {
            "id": "transfer",
            "player_id": "p1",
            "date": "2026-01-02",
            "team": "Aston Villa",
            "goals": 3,
            "assists": 2,
            "minutes": 90,
            "source": source,
        }
    )
    first = deductions.analyze(tmp_path, "2025-26", "player_rankings", teams=["Liverpool"])
    second = deductions.analyze(tmp_path, "2025-26", "player_rankings", teams=["Aston Villa"])
    assert first["rows"][0]["value"] == 2 and second["rows"][0]["value"] == 3
    combined = deductions.analyze(tmp_path, "2025-26", "player_rankings")
    assert combined["rows"][0]["value"] == 5


def test_mutated_quality_gate_is_not_reported_as_unsupported(tmp_path, monkeypatch):
    def fail(*args):
        raise ValueError("Artifact checksum mismatch")

    monkeypatch.setattr(deductions, "load_verified_corpus", fail)
    with pytest.raises(ValueError, match="checksum"):
        deductions.answer(tmp_path, "2025-26", "Liverpool points")
