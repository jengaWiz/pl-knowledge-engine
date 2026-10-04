"""Transfer attribution, cumulative snapshots and incomplete evidence regressions."""

import copy

import pytest

from src.ingest.historical_players import build_player_corpus, number


def source(row=2):
    return {
        "id": "test",
        "url": "https://example.test/archive.csv",
        "row": row,
        "sha256": "synthetic",
        "revision": "synthetic",
    }


def evidence():
    # A known complete two-club set of 38 fixtures each, with shared head-to-heads.
    names = ["Aston Villa", "Liverpool"] + [f"Team {i}" for i in range(18)]
    teams = [{"code": str(i + 1), "name": name} for i, name in enumerate(names)]
    canonical, archived, lineups, appearances = [], [], [], []
    for home_index, home in enumerate(names):
        for away_index, away in enumerate(names):
            if home == away:
                continue
            match_id = f"{home_index}:{away_index}"
            canonical.append(
                {
                    "id": match_id,
                    "date": "2025-09-01",
                    "home_team": home,
                    "away_team": away,
                    "home_score": 1,
                    "away_score": 0,
                }
            )
            archived.append(
                {
                    "match_id": match_id,
                    "tournament": "prem",
                    "finished": "True",
                    "kickoff_time": "2025-09-01T15:00:00+00:00",
                    "gameweek": "1.0",
                    "home_team": str(home_index + 1),
                    "away_team": str(away_index + 1),
                    "home_score": "1.0",
                    "away_score": "0.0",
                    "_source": source(),
                }
            )
            for index in (home_index, away_index):
                if index >= 2:
                    continue
                lineups.append(
                    {
                        "match_id": match_id,
                        "player_id": str(index + 10),
                        "team_code": str(index + 1),
                        "is_starting": "True",
                        "_source": source(),
                    }
                )
                appearances.append(
                    {
                        "match_id": match_id,
                        "player_id": str(index + 10),
                        "minutes_played": "90",
                        "goals": "0",
                        "assists": "0",
                        "xg": "0.125",
                        "xa": "0.05",
                        "_source": source(),
                    }
                )
    players = [
        {
            "player_id": str(i + 10),
            "player_code": str(i + 100),
            "first_name": "Player",
            "second_name": str(i),
            "web_name": str(i),
            "position": "Midfielder",
            # Final club deliberately differs: actual lineup must determine attribution.
            "team_code": "3",
            "_source": source(),
        }
        for i in range(2)
    ]
    snapshots = [
        {
            "id": str(i + 10),
            "gw": str(gw),
            "minutes": str(minutes),
            "total_points": str(points),
            "_source": source(gw),
        }
        for i in range(2)
        for gw, minutes, points in [(1, 90, 3), (38, 3000, 100)]
    ]
    return {
        "teams": teams,
        "players": players,
        "snapshots": snapshots,
        "archive_matches": archived,
        "lineups": lineups,
        "appearances": appearances,
        "canonical_matches": canonical,
        "season": "2025-26",
    }


def test_lineup_attribution_and_last_snapshot_preserve_metric_definitions():
    roster, appearances, report = build_player_corpus(**evidence())
    assert report["valid"] and report["team_fixture_coverage"] == {
        "Aston Villa": 38,
        "Liverpool": 38,
    }
    assert len(roster) == 2 and len(appearances) == 76
    assert roster[0]["teams_represented"] == ["Aston Villa"]
    assert roster[0]["fpl_season_totals"]["minutes"] == 3000
    assert roster[0]["fpl_season_totals"]["total_points"] == 100
    assert roster[0]["fpl_season_totals"]["goals_scored"] is None
    assert all(row["fpl_points"] is None for row in appearances)
    # Different fixtures in the same gameweek remain separate, including double gameweeks.
    assert len({row["id"] for row in appearances}) == 76
    assert all(row["gameweek"] == 1 for row in appearances)


def test_score_conflict_keeps_primary_result_and_documents_precedence():
    data = evidence()
    data["archive_matches"][0]["home_score"] = "0"
    _, _, report = build_player_corpus(**data)
    assert report["valid"]
    assert report["fixture_conflicts"][0]["primary_scores"] == [1, 0]
    assert report["fixture_conflicts"][0]["archive_scores"] == [0, 0]


def test_date_conflict_cannot_be_resolved_by_score_precedence():
    data = evidence()
    data["archive_matches"][0]["kickoff_time"] = "2026-09-01T15:00:00+00:00"
    with pytest.raises(ValueError, match="date disagrees"):
        build_player_corpus(**data)


def test_duplicate_appearance_is_not_counted_twice():
    data = evidence()
    data["appearances"].append(copy.deepcopy(data["appearances"][0]))
    with pytest.raises(ValueError, match="Duplicate player/fixture"):
        build_player_corpus(**data)


def test_missing_starting_stats_fail_but_unused_bench_is_disclosed():
    data = evidence()
    data["appearances"].pop()
    _, _, report = build_player_corpus(**data)
    assert not report["valid"] and report["lineups_without_stats"][0]["starting"]
    data = evidence()
    bench = copy.deepcopy(data["lineups"][0])
    bench.update(player_id="99", is_starting="False")
    data["lineups"].append(bench)
    _, _, report = build_player_corpus(**data)
    assert report["valid"] and not report["lineups_without_stats"][0]["starting"]


def test_missing_focus_lineup_and_unknown_identity_fail_explicitly():
    data = evidence()
    data["lineups"].pop()
    _, _, report = build_player_corpus(**data)
    assert not report["valid"] and report["missing_lineups"][0]["focus_fixture"]
    data = evidence()
    data["players"].pop()
    _, _, report = build_player_corpus(**data)
    assert not report["valid"] and report["unknown_players"] == [11]


@pytest.mark.parametrize("value", ["NaN", "inf", "-1", "text", ""])
def test_bad_numeric_evidence_is_not_replaced_with_zero(value):
    with pytest.raises(ValueError):
        number(value)


def test_integral_decimal_ids_and_optional_nulls():
    assert number("43.0", integer=True) == 43
    assert number("", optional=True) is None
    with pytest.raises(ValueError):
        number("1.5", integer=True)
