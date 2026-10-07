"""Cross-store retrieval preserves graph eligibility and exact source identity."""

import copy
from unittest.mock import MagicMock

import pytest

from src.retrieval import hybrid


@pytest.fixture
def ready(monkeypatch):
    plan = {"dataset_id": "graph-hash", "nodes": []}
    resolved = {
        "status": "resolved",
        "season": "2025-26",
        "kind": "player",
        "entity_id": "digne",
        "opponent": "Liverpool",
    }
    rows = [
        {
            "document_id": "doc",
            "match_id": "match",
            "player_id": "digne",
            "text": "verified",
            "source_refs": [{"row": 2}],
        }
    ]
    vectors = [
        {
            "id": "doc",
            "text": "verified",
            "distance": 0.2,
            "metadata": {
                "season": "2025-26",
                "match_id": "match",
                "player_id": "digne",
                "source_refs": '[{"row": 2}]',
            },
        }
    ]
    prepare = MagicMock(return_value=plan)
    graph = MagicMock(return_value=rows)
    search = MagicMock(return_value=vectors)
    monkeypatch.setattr(hybrid, "prepare_graph", prepare)
    monkeypatch.setattr(hybrid, "route", lambda *args: resolved)
    monkeypatch.setattr(hybrid, "read", graph)
    monkeypatch.setattr(hybrid, "search", search)
    return resolved, rows, vectors, prepare, graph, search


def retrieve(tmp_path, **options):
    return hybrid.retrieve(
        tmp_path,
        "2025-26",
        "Digne against Liverpool",
        "bolt://localhost:1",
        "neo4j",
        "test",
        evidence_season="2025-26",
        **options,
    )


def test_graph_first_filters_and_exact_join(tmp_path, ready):
    result = retrieve(tmp_path)
    assert result["hits"][0]["graph"] == ready[1][0]
    assert result["graph_candidates"] == 1
    assert ready[5].call_args.kwargs["match_ids"] == ("match",)
    assert ready[5].call_args.kwargs["player_id"] == "digne"
    assert ready[4].call_args.kwargs["opponent"] == "Liverpool"
    assert ready[4].call_args.kwargs["limit"] == 50


@pytest.mark.parametrize("status", ["clarification", "unsupported", "unavailable"])
def test_unresolved_routes_do_not_access_either_store(tmp_path, ready, status):
    ready[0]["status"] = status
    assert retrieve(tmp_path)["hits"] == []
    ready[4].assert_not_called()
    ready[5].assert_not_called()


def test_empty_graph_never_broadens_vector_search(tmp_path, ready):
    ready[1].clear()
    assert retrieve(tmp_path)["route"]["status"] == "unavailable"
    ready[5].assert_not_called()


@pytest.mark.parametrize("mutation", ["id", "text", "season", "match", "player", "source", "count"])
def test_mismatched_stores_fail_closed(tmp_path, ready, mutation):
    hit = ready[2][0]
    if mutation == "id":
        hit["id"] = "wrong"
    elif mutation == "text":
        hit["text"] = "wrong"
    elif mutation == "count":
        ready[2].clear()
    else:
        key = {
            "season": "season",
            "match": "match_id",
            "player": "player_id",
            "source": "source_refs",
        }[mutation]
        hit["metadata"][key] = "[]" if mutation == "source" else "wrong"
    with pytest.raises(ValueError, match="Vector and graph"):
        retrieve(tmp_path)


def test_duplicate_graph_documents_fail(tmp_path, ready):
    ready[1].append(copy.deepcopy(ready[1][0]))
    with pytest.raises(ValueError, match="duplicated"):
        retrieve(tmp_path)


def test_source_change_across_stores_fails(tmp_path, ready):
    ready[3].side_effect = [{"dataset_id": "before"}, {"dataset_id": "after"}]
    with pytest.raises(ValueError, match="changed during"):
        retrieve(tmp_path)


@pytest.mark.parametrize("limit", [0, 51, True, 1.5])
def test_bounds_before_access(tmp_path, limit):
    with pytest.raises(ValueError):
        retrieve(tmp_path, limit=limit)


def test_fixture_route_has_no_player_filter(tmp_path, ready):
    ready[0].update(kind="fixture", entity_id="match", opponent="")
    retrieve(tmp_path)
    assert ready[5].call_args.kwargs["kind"] == "match"
    assert ready[5].call_args.kwargs["player_id"] == ""


def test_duplicate_vector_document_identity_fails(tmp_path, ready):
    ready[1].append({**ready[1][0], "document_id": "second"})
    ready[2].append(copy.deepcopy(ready[2][0]))
    with pytest.raises(ValueError, match="Vector and graph"):
        retrieve(tmp_path)
