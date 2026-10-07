"""Bounded reads fail closed and keep user values in Cypher parameters."""

import json
from unittest.mock import MagicMock

import pytest

from src.store import evidence_queries as queries


@pytest.mark.parametrize(
    "options",
    [
        {"limit": 0},
        {"limit": 51},
        {"limit": True},
        {"max_hops": 0},
        {"max_hops": 5},
        {"max_hops": "4] DELETE n"},
        {"kind": "unknown"},
        {"evidence_season": ""},
        {"kind": "path"},
        {"kind": "path", "end_id": "match"},
    ],
)
def test_invalid_bounds_fail_before_access(tmp_path, options):
    args = {"evidence_season": "2025-26", "entity_id": "match", **options}
    with pytest.raises(ValueError):
        queries.read(tmp_path, "2025-26", "bolt://localhost:1", "neo4j", "test", **args)
    assert not list(tmp_path.iterdir())


@pytest.fixture
def ready(tmp_path, monkeypatch):
    plan = {
        "owner": "owner",
        "dataset_id": "hash",
        "counts": {"Entity": 3},
        "relationships": 2,
        "nodes": [
            {
                "kind": "Entity",
                "props": {"canonical_id": name, "season": "2025-26", "entity_type": kind},
            }
            for name, kind in [("match", "Match"), ("player", "Player"), ("team", "Team")]
        ],
    }
    path = queries.report_path(tmp_path, "2025-26")
    path.parent.mkdir(parents=True)
    report = {
        "valid": True,
        "schema": queries.SCHEMA,
        "dataset_id": "hash",
        "counts": plan["counts"],
        "relationships": 2,
        "canonical_links": 1,
    }
    path.write_text(json.dumps(report))
    monkeypatch.setattr(queries, "prepare_graph", lambda *args: plan)
    monkeypatch.setattr(queries, "verify_graph", lambda *args: 1)
    driver = MagicMock()
    session = driver.__enter__.return_value.session.return_value.__enter__.return_value
    tx = MagicMock()
    tx.run.return_value.data.return_value = [{"source_refs_json": '[{"row": 2}]'}]
    session.execute_read.side_effect = lambda fn: fn(tx)
    factory = MagicMock(return_value=driver)
    monkeypatch.setattr(queries.GraphDatabase, "driver", factory)
    return tmp_path, path, report, tx, factory


def run(ready, **options):
    return queries.read(
        ready[0],
        "2025-26",
        "bolt://localhost:1",
        "neo4j",
        "test",
        evidence_season=options.pop("evidence_season", "2025-26"),
        entity_id=options.pop("entity_id", "match"),
        **options,
    )


@pytest.mark.parametrize("change", ["valid", "schema", "dataset_id", "counts", "relationships"])
def test_stale_report_never_connects(ready, change):
    report = ready[2]
    report[change] = False if change == "valid" else "wrong"
    ready[1].write_text(json.dumps(report))
    with pytest.raises(ValueError, match="stale"):
        run(ready)
    ready[4].assert_not_called()


@pytest.mark.parametrize(
    "options",
    [
        {"evidence_season": "2024-25"},
        {"entity_id": "unknown"},
        {"entity_id": "player"},
        {"kind": "player"},
        {"kind": "path", "end_id": "unknown"},
    ],
)
def test_unknown_or_wrong_entity_never_connects(ready, options):
    with pytest.raises(ValueError, match="unavailable"):
        run(ready, **options)
    ready[4].assert_not_called()


def test_fixture_preserves_source_rows(ready):
    assert run(ready) == [{"source_refs": [{"row": 2}]}]
    cypher = ready[3].run.call_args.args[0]
    assert cypher == queries.FIXTURE
    assert ready[3].run.call_args.kwargs["evidence_season"] == "2025-26"


def test_opponent_is_a_parameter(ready):
    opponent = "Liverpool' DELETE n"
    run(ready, kind="player", entity_id="player", opponent=opponent)
    call = ready[3].run.call_args
    assert call.args[0] == queries.PLAYER and opponent not in call.args[0]
    assert call.kwargs["opponent"] == opponent


def test_path_is_bounded_and_scoped(ready):
    run(ready, kind="path", end_id="team", max_hops=3)
    text = ready[3].run.call_args.args[0]
    assert "*1..3" in text and "LIMIT 1" in text
    assert "all(n IN nodes(p)" in text and "all(r IN relationships(p)" in text


def test_changed_bridges_fail_before_query(ready, monkeypatch):
    monkeypatch.setattr(queries, "verify_graph", lambda *args: 0)
    with pytest.raises(ValueError, match="references changed"):
        run(ready)
    ready[3].run.assert_not_called()
