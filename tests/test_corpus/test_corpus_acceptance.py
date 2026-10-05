"""Combined acceptance must retain source gates and distinguish preparation from targets."""

import json

import pytest

from src.corpus import acceptance as gate
from src.corpus.contracts import Asset, Document, content_id, document_id, sha256


def asset(content, modality, **updates):
    checksum = sha256(content)
    return Asset(
        **{
            "id": content_id("asset", checksum),
            "sha256": checksum,
            "modality": modality,
            "mime_type": {
                "text": "text/plain",
                "audio": "audio/wav",
                "image": "image/png",
                "video": "video/mp4",
            }[modality],
            "bytes": len(content),
            "source_url": "https://example.org/source",
            "reference_url": "https://example.org/terms",
            "publisher": "Publisher",
            "attribution": "Author",
            "license_name": "Terms",
            "license_url": "https://example.org/terms",
            "scope": "background",
            **updates,
        }
    )


def record(parent, content, **updates):
    checksum = sha256(content)
    modality = parent.modality
    end = len(content.decode()) if modality == "text" else 1
    doc = Document(
        id=document_id(parent.id, modality, 0, end, "test", checksum),
        asset_id=parent.id,
        modality=modality,
        sha256=checksum,
        extraction_version="test",
        start=0,
        end=end,
        coordinate={"text": "characters", "image": "whole_image"}.get(modality, "seconds"),
    )
    return {"document": doc.model_dump(), **updates}


@pytest.fixture
def sources(monkeypatch):
    original = asset(b"csv", "text")
    content = "Match 1–0".encode()
    derived = asset(
        content,
        "text",
        parent_asset_id=original.id,
        scope="seasonal",
        season="2025-26",
        event_date="2025-09-01",
    )
    text_records = [record(derived, content, text=content.decode())]
    media_assets = [asset(modality.encode(), modality) for modality in ("audio", "image", "video")]
    media_records = [
        record(
            parent,
            parent.modality.encode(),
            bytes=parent.bytes,
            suffix={"audio": ".wav", "image": ".png", "video": ".mp4"}[parent.modality],
        )
        for parent in media_assets
    ]
    snapshot = {"text": "verified", "media": "verified"}
    monkeypatch.setattr(gate, "input_snapshot", lambda output, season: dict(snapshot))
    monkeypatch.setattr(
        gate, "load_text_documents", lambda output, season: ([original, derived], text_records)
    )
    monkeypatch.setattr(gate, "load_media_documents", lambda output: (media_assets, media_records))
    return [original, derived], text_records, media_assets, media_records, snapshot


def test_combined_counts_scope_and_repeat(tmp_path, sources):
    report = gate.accept_corpus(tmp_path, "2025-26")
    assert report["valid"] and report["status"] == "prepared_below_target"
    assert report["counts"]["retrieval_documents"] == 4
    assert report["counts"]["source_assets"] == 4 and report["counts"]["derived_assets"] == 1
    assert report["counts"]["documents_by_modality"] == {
        "audio": 1,
        "image": 1,
        "text": 1,
        "video": 1,
    }
    assert report["document_records_by_scope"] == {"background": 3, "seasonal": 1}
    assert report["seasonal_date_range"] == ["2025-09-01", "2025-09-01"]
    assert (
        report["documents_short_of_target"] == 2496 and report["embeddings_created_by_stage"] == 0
    )
    first = gate.paths(tmp_path, "2025-26")[1].read_bytes()
    gate.accept_corpus(tmp_path, "2025-26")
    assert gate.paths(tmp_path, "2025-26")[1].read_bytes() == first
    assets, records, summary = gate.load_corpus(tmp_path, "2025-26")
    assert len(assets) == 5 and len(records) == 4 and summary["counts"] == report["counts"]


def test_parent_conflict_rejected(sources):
    assets = sources[0]
    changed = assets[0].model_copy(update={"publisher": "Conflicting publisher"})
    with pytest.raises(ValueError, match="conflicting provenance"):
        gate.combine([assets, [changed]], [])


def test_orphaned_document_rejected(sources):
    with pytest.raises(ValueError, match="parent"):
        gate.combine([], [("text", sources[1])])


def test_duplicate_content_keeps_traceability_without_inflation(tmp_path, sources):
    parent = sources[0][1]
    content = sources[1][0]["text"].encode()
    second = record(parent, content, text=content.decode())
    second["document"]["extraction_version"] = "second-location"
    d = second["document"]
    d["id"] = document_id(
        d["asset_id"], "text", d["start"], d["end"], "second-location", d["sha256"]
    )
    sources[1].append(second)
    report = gate.accept_corpus(tmp_path, "2025-26")
    assert report["valid"] and report["document_records"] == 5
    assert report["counts"]["retrieval_documents"] == 4
    assert report["counts"]["duplicate_document_content"] == 1
    assert report["payload_bytes"] == len(content) + sum(a.bytes for a in sources[2])


def test_moving_input_snapshot_fails_closed(tmp_path, sources, monkeypatch):
    def change(output):
        sources[4]["media"] = "new-version"
        return sources[2], sources[3]

    monkeypatch.setattr(gate, "load_media_documents", change)
    report = gate.accept_corpus(tmp_path, "2025-26")
    assert not report["valid"] and "changed during" in report["detail"]
    assert not gate.paths(tmp_path, "2025-26")[1].exists()


def test_changed_stage_invalidates_previous_acceptance(tmp_path, sources):
    gate.accept_corpus(tmp_path, "2025-26")
    sources[4]["text"] = "changed"
    with pytest.raises(ValueError, match="accepted inputs"):
        gate.load_corpus(tmp_path, "2025-26")


def test_upstream_failure_withdraws_combined_manifest(tmp_path, sources, monkeypatch):
    gate.accept_corpus(tmp_path, "2025-26")

    def reject(output):
        raise ValueError("Upstream extraction is invalid")

    monkeypatch.setattr(gate, "load_media_documents", reject)
    assert not gate.accept_corpus(tmp_path, "2025-26")["valid"]
    with pytest.raises(ValueError, match="incomplete"):
        gate.load_corpus(tmp_path, "2025-26")


@pytest.mark.parametrize("limits", [{"document_budget": 1}, {"byte_budget": 1}])
def test_budget_failure_invalidates_previous_acceptance(tmp_path, sources, limits):
    gate.accept_corpus(tmp_path, "2025-26")
    report = gate.accept_corpus(tmp_path, "2025-26", **limits)
    assert not report["valid"] and "budget" in report["detail"]
    assert not gate.paths(tmp_path, "2025-26")[1].exists()


def test_report_cannot_claim_false_target(tmp_path, sources):
    gate.accept_corpus(tmp_path, "2025-26")
    report_path = gate.paths(tmp_path, "2025-26")[2]
    report = json.loads(report_path.read_text())
    report["status"] = "prepared_target_met"
    report["counts"]["multimodal_target_met"] = True
    report_path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match="accepted inputs"):
        gate.load_corpus(tmp_path, "2025-26")


def test_manifest_tampering_rejected(tmp_path, sources):
    gate.accept_corpus(tmp_path, "2025-26")
    docs_path = gate.paths(tmp_path, "2025-26")[1]
    docs_path.write_bytes(docs_path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="manifest changed"):
        gate.load_corpus(tmp_path, "2025-26")
