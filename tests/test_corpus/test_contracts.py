"""Corpus counts cannot inflate source assets or unsupported modality coverage."""

import pytest
from pydantic import ValidationError

from src.corpus.contracts import Asset, Document, content_id, count_corpus, document_id, sha256


def asset(content=b"original", modality="video", **overrides):
    checksum = sha256(content)
    return Asset(
        **{
            "id": content_id("asset", checksum),
            "sha256": checksum,
            "modality": modality,
            "mime_type": {
                "video": "video/mp4",
                "audio": "audio/wav",
                "image": "image/png",
                "text": "text/plain",
            }[modality],
            "bytes": len(content),
            "source_url": "https://example.org/media",
            "reference_url": "https://example.org/license",
            "publisher": "Publisher",
            "attribution": "Author",
            "license_name": "CC BY 4.0",
            "license_url": "https://creativecommons.org/licenses/by/4.0/",
            "scope": "background",
            **overrides,
        }
    )


def document(parent, content=b"segment", start=0, end=1):
    checksum = sha256(content)
    return Document(
        id=document_id(parent.id, parent.modality, start, end, "v1", checksum),
        asset_id=parent.id,
        modality=parent.modality,
        sha256=checksum,
        extraction_version="v1",
        start=start,
        end=end,
        coordinate={"text": "characters", "image": "whole_image"}.get(parent.modality, "seconds"),
    )


def test_extracted_audio_does_not_create_an_independent_source():
    video = asset()
    audio = asset(b"audio", "audio", parent_asset_id=video.id)
    report = count_corpus([video, audio], [document(video), document(audio)])
    assert report["source_assets"] == 1
    assert report["derived_assets"] == 1
    assert report["source_assets_by_modality"]["audio"] == 0
    assert report["documents_by_modality"]["audio"] == 1
    assert not report["multimodal_target_met"]


def test_repeated_chunks_do_not_inflate_counts():
    parent = asset(b"text", "text")
    docs = [document(parent, start=i, end=i + 1) for i in range(2500)]
    report = count_corpus([parent], docs)
    assert report["retrieval_documents"] == 1
    assert report["duplicate_document_content"] == 2499
    assert not report["multimodal_target_met"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"id": "made-up"},
        {"bytes": 0},
        {"mime_type": "image/png"},
        {"scope": "seasonal", "season": "2025-26"},
        {"scope": "seasonal", "season": "2025-26", "event_date": "2024-01-01"},
        {"season": "2025-26"},
        {"license_name": ""},
        {"source_url": "https://user:secret@example.org/media"},
    ],
)
def test_invalid_asset_evidence_rejected(overrides):
    with pytest.raises(ValidationError):
        asset(**overrides)


def test_missing_parent_rejected():
    parent = asset()
    child = asset(b"child", parent_asset_id=parent.id)
    with pytest.raises(ValueError, match="Missing parent"):
        count_corpus([child], [])


def test_cyclic_derivations_rejected():
    first = asset(b"first")
    second = asset(b"second", parent_asset_id=first.id)
    first = asset(b"first", parent_asset_id=second.id)
    with pytest.raises(ValueError, match="cyclic"):
        count_corpus([first, second], [])


def test_duplicate_ids_and_orphan_documents_rejected():
    parent = asset()
    doc = document(parent)
    with pytest.raises(ValueError, match="Duplicate"):
        count_corpus([parent, parent], [doc])
    with pytest.raises(ValueError, match="Duplicate"):
        count_corpus([parent], [doc, doc])
    with pytest.raises(ValueError, match="parent"):
        count_corpus([], [doc])


@pytest.mark.parametrize("start,end", [(1, 1), (2, 1), (0, float("inf")), (0, float("nan"))])
def test_invalid_extraction_range_rejected(start, end):
    with pytest.raises(ValidationError):
        document(asset(), start=start, end=end)


def test_all_modalities_and_threshold_required():
    parents = [asset(m.encode(), m) for m in ["text", "image", "audio", "video"]]
    docs = [document(p, p.modality.encode()) for p in parents]
    text = parents[0]
    docs += [document(text, f"unique-{i}".encode(), start=i + 1, end=i + 2) for i in range(2496)]
    assert count_corpus(parents, docs)["multimodal_target_met"]
    assert not count_corpus(parents, docs[:-1])["multimodal_target_met"]
