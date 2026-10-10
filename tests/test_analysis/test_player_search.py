"""Player identity matching must tolerate names without inventing roster coverage."""

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from backend import graph
from backend.player_search import resolve_player

ROSTER = [
    {"id": "p1", "name": "Ekitiké", "full_name": "Hugo Ekitiké"},
    {"id": "p2", "name": "A.Becker", "full_name": "Alisson Becker"},
    {"id": "p3", "name": "M.Salah", "full_name": "Mohamed Salah"},
    {"id": "p4", "name": "Alysson", "full_name": "Alysson Edward Franco"},
    {"id": "p5", "name": "Douglas Luiz", "full_name": "Douglas Luiz Soares de Paulo"},
]


@pytest.mark.parametrize(
    "query,ident",
    [
        ("ekitike", "p1"),
        (" HUGO EKITIKÉ ", "p1"),
        ("alisson", "p2"),
        ("a becker", "p2"),
        ("salah", "p3"),
        ("m.salah", "p3"),
        ("p4", "p4"),
    ],
)
def test_normalized_identity_matches(query, ident):
    result = resolve_player(query, ROSTER)
    assert result["status"] == "matched" and result["player"]["id"] == ident


@pytest.mark.parametrize("query,ident", [("allison", "p2"), ("ekitke", "p1"), ("salllah", "p3")])
def test_typo_returns_explicit_suggestions_not_an_automatic_identity(query, ident):
    result = resolve_player(query, ROSTER)
    assert result["status"] == "suggestions" and "player" not in result
    assert result["suggestions"][0]["id"] == ident


@pytest.mark.parametrize("query", ["Luis Diaz", "unrelatedperson", "", " ", "a", "x" * 121])
def test_missing_or_unbounded_names_do_not_invent_matches(query):
    assert resolve_player(query, ROSTER) == {"status": "missing", "suggestions": []}


def test_ambiguous_names_require_selection_and_bound_suggestions():
    roster = [{"id": str(i), "name": "Jones", "full_name": f"Player {i} Jones"} for i in range(10)]
    result = resolve_player("Jones", roster)
    assert result["status"] == "ambiguous" and len(result["suggestions"]) == 5
    assert resolve_player("0", roster)["player"]["id"] == "0"


def test_roster_order_does_not_change_suggestion_ranking():
    assert resolve_player("allison", ROSTER) == resolve_player("allison", ROSTER[::-1])


def test_graph_search_uses_resolved_canonical_id(monkeypatch):
    monkeypatch.setattr(graph, "connect", MagicMock())
    reads = MagicMock(
        side_effect=[
            ROSTER,
            [
                {
                    "id": "n1",
                    "type": "Player",
                    "props": {"web_name": "Ekitiké", "full_name": "Hugo Ekitiké"},
                }
            ],
            [],
        ]
    )
    monkeypatch.setattr(graph, "query", reads)
    result = graph.read_graph("player", "ekitike")
    assert reads.call_args_list[1].kwargs["id"] == "p1"
    assert result["nodes"][0]["full_name"] == "Hugo Ekitiké"


@pytest.mark.parametrize("query,code", [("allison", 409), ("Luis Diaz", 404)])
def test_graph_errors_distinguish_suggestions_from_missing_coverage(monkeypatch, query, code):
    monkeypatch.setattr(graph, "connect", MagicMock())
    reads = MagicMock(return_value=ROSTER)
    monkeypatch.setattr(graph, "query", reads)
    with pytest.raises(HTTPException) as error:
        graph.read_graph("player", query)
    assert error.value.status_code == code
    assert "2025–26" in error.value.detail["coverage"]
    assert bool(error.value.detail["suggestions"]) == (code == 409)
    assert reads.call_count == 1
