"""Local evidence indexing must resume verified batches and enforce source/model scope."""

import json
from types import SimpleNamespace

import pytest

from src.corpus.contracts import sha256
from src.store import evidence_index as index


def vectors(texts):
    return [[1.0] + [0.0] * 383 for _ in texts]


@pytest.fixture
def corpus(monkeypatch):
    assets, records = [], []
    for number in range(35):
        text = f"Match evidence {number}"
        season = "2024-25" if number < 32 else "2025-26"
        parent = SimpleNamespace(
            id=f"asset{number}", scope="seasonal", source_url="https://example.org/source"
        )
        assets.append(parent)
        records.append(
            {
                "document": {
                    "id": f"doc{number:03}",
                    "asset_id": parent.id,
                    "modality": "text",
                    "sha256": sha256(text.encode()),
                },
                "text": text,
                "kind": "match",
                "record_ids": [f"match{number}"],
                "season": season,
                "event_date": season[:4] + "-09-01",
                "source_refs": [
                    {
                        "id": "matches",
                        "row": number + 2,
                        "url": parent.source_url,
                        "revision": "pin",
                    }
                ],
            }
        )
    summary = {
        "target_met": True,
        "counts": {"retrieval_documents": 35},
        "target_policy": {"mode": "structured", "minimum_unique_documents": 2500},
    }
    monkeypatch.setattr(index, "load_corpus", lambda *args: (assets, records, summary))
    monkeypatch.setattr(index, "embedding_function", lambda: vectors)
    return assets, records, summary


def test_persistent_batches_repeat_and_season_filters(tmp_path, corpus):
    calls = []

    def embed(texts):
        calls.append(len(texts))
        return vectors(texts)

    first = index.build_index(tmp_path, "2025-26", embed=embed)
    second = index.build_index(tmp_path, "2025-26", embed=embed)
    assert calls == [32, 3] and second["reused_batches"] == 2
    assert first["dataset_id"] == second["dataset_id"]
    assert first["indexed_records"] == 35 and first["records_by_season"] == {
        "2024-25": 32,
        "2025-26": 3,
    }
    hits = index.search(tmp_path, "2025-26", "Match", evidence_season="2025-26")
    assert len(hits) == 3 and all(hit["metadata"]["season"] == "2025-26" for hit in hits)
    assert json.loads(hits[0]["metadata"]["source_refs"])[0]["revision"] == "pin"
    assert (
        index.search(tmp_path, "2025-26", "Match", evidence_season="2024-25", match_id="match0")[0][
            "id"
        ]
        == "doc000"
    )
    assert (
        index.search(tmp_path, "2025-26", "Match", evidence_season="2024-25", player_id="absent")
        == []
    )


def test_failed_batch_resumes_previous_verified_work(tmp_path, corpus):
    calls = []

    def fail_second(texts):
        calls.append(len(texts))
        if len(calls) == 2:
            raise RuntimeError("Interrupted embedding")
        return vectors(texts)

    with pytest.raises(RuntimeError):
        index.build_index(tmp_path, "2025-26", embed=fail_second)
    assert not json.loads(index.report_path(tmp_path, "2025-26").read_text())["valid"]
    resumed = []
    report = index.build_index(
        tmp_path, "2025-26", embed=lambda texts: resumed.append(len(texts)) or vectors(texts)
    )
    assert resumed == [3] and report["reused_batches"] == 1


@pytest.mark.parametrize(
    "embed",
    [
        lambda texts: [],
        lambda texts: [[1.0] for _ in texts],
        lambda texts: [[float("nan")] * 384 for _ in texts],
        lambda texts: [[0.0] * 384 for _ in texts],
    ],
)
def test_invalid_vectors_never_publish_ready_report(tmp_path, corpus, embed):
    with pytest.raises(ValueError):
        index.build_index(tmp_path, "2025-26", embed=embed)
    assert not json.loads(index.report_path(tmp_path, "2025-26").read_text())["valid"]


def test_same_count_vector_mutation_fails_query_and_resume(tmp_path, corpus):
    report = index.build_index(tmp_path, "2025-26")
    collection = index.client(tmp_path).get_collection(
        report["collection"], embedding_function=None
    )
    collection.update(ids=["doc000"], embeddings=[[0.0, 1.0] + [0.0] * 382])
    with pytest.raises(ValueError, match="vectors or checkpoint"):
        index.search(tmp_path, "2025-26", "Match", evidence_season="2024-25")
    with pytest.raises(ValueError, match="Cached index batch"):
        index.build_index(tmp_path, "2025-26")


def test_same_count_text_mutation_fails_query(tmp_path, corpus):
    report = index.build_index(tmp_path, "2025-26")
    collection = index.client(tmp_path).get_collection(
        report["collection"], embedding_function=None
    )
    collection.update(ids=["doc000"], documents=["Different evidence"])
    with pytest.raises(ValueError, match="text or provenance"):
        index.search(tmp_path, "2025-26", "Match", evidence_season="2024-25")


def test_changed_source_records_make_index_stale(tmp_path, corpus):
    index.build_index(tmp_path, "2025-26")
    corpus[1][0]["source_refs"][0]["revision"] = "newpin"
    with pytest.raises(ValueError, match="stale"):
        index.search(tmp_path, "2025-26", "Match", evidence_season="2024-25")
    assert index.build_index(tmp_path, "2025-26")["reused_batches"] == 0


def test_collection_metadata_model_mismatch_rejected(tmp_path, corpus):
    report = index.build_index(tmp_path, "2025-26")
    collection = index.client(tmp_path).get_collection(
        report["collection"], embedding_function=None
    )
    collection.modify(
        metadata={
            **{k: v for k, v in collection.metadata.items() if k != "hnsw:space"},
            "model_sha256": "changed",
        }
    )
    with pytest.raises(ValueError, match="model or dataset"):
        index.search(tmp_path, "2025-26", "Match", evidence_season="2024-25")


def test_unrelated_collections_survive_rebuild(tmp_path, corpus):
    other = index.client(tmp_path).create_collection("unrelated", embedding_function=None)
    other.add(ids=["keep"], embeddings=[[1.0] + [0.0] * 383], documents=["Keep me"])
    index.build_index(tmp_path, "2025-26")
    assert index.client(tmp_path).get_collection("unrelated", embedding_function=None).count() == 1
    assert not (tmp_path / "stores/chroma").exists()


@pytest.mark.parametrize(
    "query,season,limit",
    [("", "2025-26", 5), ("Match", "", 5), ("Match", "2025-26", 0), ("x" * 4001, "2025-26", 5)],
)
def test_search_bounds_before_data_access(tmp_path, query, season, limit):
    with pytest.raises(ValueError):
        index.search(tmp_path, "2025-26", query, evidence_season=season, limit=limit)


def test_unknown_season_cannot_fall_back_to_other_evidence(tmp_path, corpus):
    index.build_index(tmp_path, "2025-26")
    with pytest.raises(ValueError, match="no accepted evidence"):
        index.search(tmp_path, "2025-26", "Match", evidence_season="2020-21")


def test_unaccepted_target_cannot_be_indexed(tmp_path, corpus):
    corpus[2]["target_met"] = False
    with pytest.raises(ValueError, match="target must pass"):
        index.build_index(tmp_path, "2025-26")


def test_nonunit_vectors_are_normalized_before_persistence(tmp_path, corpus):
    import numpy as np

    report = index.build_index(
        tmp_path, "2025-26", embed=lambda texts: [[3.0, 4.0] + [0.0] * 382 for _ in texts]
    )
    collection = index.client(tmp_path).get_collection(
        report["collection"], embedding_function=None
    )
    stored = collection.get(ids=["doc000"], include=["embeddings"])["embeddings"][0]
    assert np.linalg.norm(stored) == pytest.approx(1.0)
    assert stored[:2] == pytest.approx([0.6, 0.8])


def test_nontext_input_cannot_be_indexed_as_text(tmp_path, corpus):
    corpus[1][0]["document"]["modality"] = "video"
    with pytest.raises(ValueError, match="checksum-verified text"):
        index.build_index(tmp_path, "2025-26")
