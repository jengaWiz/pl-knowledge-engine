"""Owned graph projections preserve season, source lineage and unrelated MVP state."""

import copy
import json
from unittest.mock import MagicMock

import pytest

from src.corpus.contracts import Asset, content_id, sha256
from src.store import evidence_graph as graph


@pytest.fixture
def evidence():
    raw = b"original CSV"
    checksum = sha256(raw)
    original = Asset(
        id=content_id("asset", checksum),
        sha256=checksum,
        modality="text",
        mime_type="text/csv",
        bytes=len(raw),
        source_url="https://example.org/data.csv",
        reference_url="https://example.org/terms",
        publisher="Publisher",
        attribution="Author",
        license_name="Terms",
        license_url="https://example.org/terms",
        scope="background",
    )
    current_match = {
        "id": "match2025",
        "season": "2025-26",
        "date": "2025-09-01",
        "home_team": "Liverpool",
        "away_team": "Aston Villa",
        "home_score": 1,
        "away_score": 0,
    }
    historical = [{**current_match, "id": "match2024", "season": "2024-25", "date": "2024-09-01"}]
    player = {
        "id": "player2025",
        "season": "2025-26",
        "name": "Example Player",
        "position": "Defender",
    }
    app = {
        "id": "appearance2025",
        "season": "2025-26",
        "date": "2025-09-01",
        "player_id": player["id"],
        "match_id": current_match["id"],
        "team": "Liverpool",
        "minutes": 90,
        "goals": None,
        "assists": 0,
        "expected_goals": None,
        "expected_assists": 0.1,
    }
    current = {"matches": [current_match], "players": [player], "appearances": [app]}
    assets, records = [original], []
    for index, (kind, ids, season) in enumerate(
        [
            ("match", ["match2025"], "2025-26"),
            ("match", ["match2024"], "2024-25"),
            ("appearance", ["appearance2025", "player2025", "match2025"], "2025-26"),
        ]
    ):
        text = f"Evidence {index}"
        digest = sha256(text.encode())
        derived = Asset(
            **{
                **original.model_dump(),
                "id": content_id("asset", digest),
                "sha256": digest,
                "mime_type": "text/plain",
                "bytes": len(text),
                "parent_asset_id": original.id,
                "scope": "seasonal",
                "season": season,
                "event_date": season[:4] + "-09-01",
            }
        )
        assets.append(derived)
        records.append(
            {
                "kind": kind,
                "record_ids": ids,
                "season": season,
                "event_date": season[:4] + "-09-01",
                "text": text,
                "document": {"id": f"doc{index}", "asset_id": derived.id, "sha256": digest},
                "source_refs": [
                    {
                        "asset_id": original.id,
                        "id": "matches",
                        "row": index + 2,
                        "url": original.source_url,
                        "revision": "pin",
                    }
                ],
            }
        )
    return assets, records, current, historical


def plan(evidence):
    return graph.graph_plan(*evidence, "2025-26")


def test_canonical_projection_source_counts_and_distinct_seasons(evidence):
    prepared = plan(evidence)
    assert prepared["counts"] == {"Corpus": 1, "Document": 3, "Entity": 8, "Season": 2, "Source": 1}
    assert prepared["entity_counts"] == {"Match": 2, "Team": 4, "Player": 1, "PlayerAppearance": 1}
    assert prepared["relationships"] == 40
    assert all(not row["props"].get("mvp_managed") for row in prepared["nodes"])
    teams = [row for row in prepared["nodes"] if row["props"].get("entity_type") == "Team"]
    assert len({row["props"]["canonical_id"] for row in teams}) == 4
    assert plan(evidence) == prepared
    edges = {row["type"] for row in prepared["edges"]}
    assert {"IN_MATCH", "OF_PLAYER", "FOR_TEAM", "HOME_TEAM", "AWAY_TEAM", "CITES"} <= edges


def test_document_keeps_all_rows_without_duplicate_source_edges(evidence):
    refs = evidence[1][0]["source_refs"]
    refs.append({**refs[0], "row": 99})
    prepared = plan(evidence)
    citations = [row for row in prepared["edges"] if row["type"] == "CITES"]
    assert len(citations) == 3
    assert any(len(json.loads(row["props"]["refs_json"])) == 2 for row in citations)


def test_derived_asset_cannot_impersonate_original_citation(evidence):
    evidence[1][0]["source_refs"][0]["asset_id"] = evidence[0][1].id
    with pytest.raises(ValueError, match="original source"):
        plan(evidence)


def test_unknown_metrics_stay_absent_in_neo4j_properties(evidence):
    prepared = plan(evidence)
    app = next(
        row for row in prepared["nodes"] if row["props"].get("entity_type") == "PlayerAppearance"
    )
    props = graph.properties(prepared, app)
    assert "goals" not in props and "expected_goals" not in props
    assert props["assists"] == 0 and props["expected_assists"] == 0.1


def test_changed_evidence_versions_graph(evidence):
    before = plan(evidence)["dataset_id"]
    evidence[1][0]["source_refs"][0]["revision"] = "new-pin"
    assert plan(evidence)["dataset_id"] != before


def fake_tx(prepared, *, nodes=None, edges=None, bridges=None):
    default_nodes = [
        {
            "key": row["key"],
            "props": graph.properties(prepared, row),
            "labels": ["EvidenceNode", "Evidence" + row["kind"]],
        }
        for row in prepared["nodes"]
    ]
    default_edges = [
        {
            "start": row["start"],
            "end": row["end"],
            "type": row["type"],
            "props": graph.properties(prepared, row),
        }
        for row in prepared["edges"]
    ]
    tx = MagicMock()

    def run(query, **params):
        result = MagicMock()
        if "labels(n)" in query:
            result.data.return_value = default_nodes if nodes is None else nodes
        elif "type(r)<>'CANONICAL_RECORD'" in query:
            result.data.return_value = default_edges if edges is None else edges
        else:
            result.data.return_value = bridges or []
        return result

    tx.run.side_effect = run
    return tx, default_nodes, default_edges


def test_complete_graph_verification(evidence):
    prepared = plan(evidence)
    tx, _, _ = fake_tx(prepared)
    assert graph.verify_graph(tx, prepared) == 0


@pytest.mark.parametrize(
    "change",
    ["property", "missing_node", "label", "missing_edge", "edge_provenance", "duplicate_edge"],
)
def test_same_count_mutation_or_dangling_graph_fails(evidence, change):
    prepared = plan(evidence)
    _, nodes, edges = fake_tx(prepared)
    nodes, edges = copy.deepcopy(nodes), copy.deepcopy(edges)
    if change == "property":
        nodes[0]["props"]["unexpected"] = "changed"
    elif change == "missing_node":
        nodes.pop()
    elif change == "label":
        nodes[0]["labels"].pop()
    elif change == "missing_edge":
        edges.pop()
    elif change == "duplicate_edge":
        edges.append(copy.deepcopy(edges[0]))
    else:
        edges[0]["props"]["dataset_id"] = "wrong"
    tx, _, _ = fake_tx(prepared, nodes=nodes, edges=edges)
    with pytest.raises(ValueError):
        graph.verify_graph(tx, prepared)


def test_wrong_canonical_link_is_rejected(evidence):
    prepared = plan(evidence)
    tx, _, _ = fake_tx(
        prepared,
        bridges=[
            {
                "canonical_id": "player2025",
                "entity_type": "Player",
                "labels": ["Player"],
                "target": {"player_id": "different"},
                "dataset": prepared["dataset_id"],
            }
        ],
    )
    with pytest.raises(ValueError, match="Canonical evidence link"):
        graph.verify_graph(tx, prepared)


def test_write_prunes_only_owned_evidence_and_never_sets_mvp_nodes(evidence, monkeypatch):
    prepared = plan(evidence)
    tx = MagicMock()
    monkeypatch.setattr(graph, "verify_graph", lambda *args: 0)
    graph.write_graph(tx, prepared)
    calls = [(call.args[0], call.kwargs) for call in tx.run.call_args_list]
    deletes = [(text, params) for text, params in calls if "DELETE" in text]
    assert len(deletes) == 2 and all(params["owner"] == prepared["owner"] for _, params in deletes)
    assert all("evidence_owner" in text and "$owner" in text for text, _ in deletes)
    assert all("SET b" not in text for text, _ in calls)
    assert all("MATCH (n:EvidenceNode" in text for text, _ in deletes if "DETACH" in text)


def test_credentials_required_before_data_access(tmp_path):
    with pytest.raises(ValueError, match="NEO4J_PASSWORD"):
        graph.load_graph(tmp_path, "2025-26", "bolt://localhost:1", "neo4j", "")


def test_failed_preparation_withdraws_ready_report(tmp_path, monkeypatch):
    path = graph.report_path(tmp_path, "2025-26")
    path.parent.mkdir(parents=True)
    path.write_text('{"valid":true}')
    monkeypatch.setattr(
        graph, "prepare_graph", lambda *args: (_ for _ in ()).throw(ValueError("Invalid source"))
    )
    with pytest.raises(ValueError):
        graph.load_graph(tmp_path, "2025-26", "bolt://localhost:1", "neo4j", "test")
    assert not json.loads(path.read_text())["valid"]


@pytest.mark.parametrize("mutation", [None, "duplicate", "owner", "canonical", "managed"])
def test_canonical_bridge_identity_and_ownership(evidence, mutation):
    prepared = plan(evidence)
    source = next(row for row in prepared["nodes"] if row["props"].get("entity_type") == "Player")
    bridge = {
        "source_key": source["key"],
        "source_owner": prepared["owner"],
        "canonical_id": "player2025",
        "entity_type": "Player",
        "labels": ["Player"],
        "target": {"player_id": "player2025", "mvp_managed": True},
        "dataset": prepared["dataset_id"],
    }
    if mutation == "owner":
        bridge["source_owner"] = "foreign"
    elif mutation == "canonical":
        bridge["canonical_id"] = bridge["target"]["player_id"] = "different"
    elif mutation == "managed":
        bridge["target"]["mvp_managed"] = False
    tx, _, _ = fake_tx(prepared, bridges=[bridge, bridge] if mutation == "duplicate" else [bridge])
    if mutation:
        with pytest.raises(ValueError, match="Canonical evidence link"):
            graph.verify_graph(tx, prepared)
    else:
        assert graph.verify_graph(tx, prepared) == 1
