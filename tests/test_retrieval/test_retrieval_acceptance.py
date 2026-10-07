"""Frozen smoke checks reject wrong scores, missing appearances and false abstention."""

import copy
import json

import pytest

from scripts.accept_retrieval import FIXTURES, check_result


@pytest.fixture
def fixture():
    case = json.loads(FIXTURES.read_text())["fixtures"][0]
    graph = {key: case[key] for key in ("match_id", "home_score", "away_score")}
    result = {
        "route": {"status": "resolved"},
        "hits": [
            {
                "metadata": {"season": case["season"]},
                "graph": {**graph, "source_refs": [{"row": 2}]},
            }
        ],
    }
    return case, result


def test_frozen_fixture_check(fixture):
    assert check_result("fixtures", *fixture)


@pytest.mark.parametrize("mutation", ["score", "season", "source", "extra"])
def test_fixture_check_is_independent_of_route(fixture, mutation):
    case, result = fixture
    if mutation == "score":
        result["hits"][0]["graph"]["home_score"] += 1
    elif mutation == "season":
        result["hits"][0]["metadata"]["season"] = "2025-26"
    elif mutation == "source":
        result["hits"][0]["graph"]["source_refs"] = []
    else:
        result["hits"].append(copy.deepcopy(result["hits"][0]))
    assert not check_result("fixtures", case, result)


def test_abstention_must_have_no_hits(fixture):
    case = {"status": "clarification"}
    result = {"route": case, "hits": []}
    assert check_result("abstentions", case, result)
    result["hits"] = fixture[1]["hits"]
    assert not check_result("abstentions", case, result)


def test_player_check_requires_both_opponent_appearances():
    case = json.loads(FIXTURES.read_text())["players"][0]
    result = {
        "route": {"status": "resolved"},
        "hits": [
            {
                "metadata": {"season": case["season"]},
                "graph": {
                    "match_id": match,
                    "source_refs": [{"row": 2}],
                    **{key: case[key] for key in ("player_id", "team", "opponent")},
                },
            }
            for match in case["matches"]
        ],
    }
    assert check_result("players", case, result)
    result["hits"].pop()
    assert not check_result("players", case, result)
