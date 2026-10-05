"""Extraction must resume valid chunks, preserve ranges and reject changed artifacts."""

import json
from types import SimpleNamespace

import pytest

from src.corpus import media_extraction as media
from src.corpus.contracts import Asset, content_id, sha256


@pytest.fixture
def source(tmp_path, monkeypatch):
    content = b"original audio"
    checksum = sha256(content)
    asset = Asset(
        id=content_id("asset", checksum),
        sha256=checksum,
        modality="audio",
        mime_type="audio/ogg",
        bytes=len(content),
        source_url="https://example.org/audio",
        reference_url="https://example.org/source",
        publisher="Publisher",
        attribution="Author",
        license_name="CC BY 4.0",
        license_url="https://creativecommons.org/licenses/by/4.0/",
        scope="background",
    )
    path = tmp_path / "raw/multimodal/commons" / (checksum + ".blob")
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    monkeypatch.setattr(media, "load_verified_media", lambda output: [asset])
    monkeypatch.setattr(
        media.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="ffmpeg version test\n"),
    )
    monkeypatch.setattr(media, "inspect_duration", lambda path, modality=None: 25.0)
    calls = []

    def extract(source, destination, modality, start, end):
        calls.append((start, end))
        destination.write_bytes(f"audio:{start}:{end}".encode())

    monkeypatch.setattr(media, "extract_segment", extract)
    return asset, calls, extract


def test_ranges_cover_tail_without_overlap():
    assert media.ranges(25.1, 20) == [(0, 20), (20, 25.1)]
    assert media.ranges(40, 20) == [(0, 20), (20, 40)]


@pytest.mark.parametrize(
    "duration,seconds",
    [(0, 20), (-1, 20), (float("inf"), 20), (float("nan"), 20), (25, 1), (25, 61)],
)
def test_bad_duration_or_limits_rejected(duration, seconds):
    with pytest.raises(ValueError):
        media.ranges(duration, seconds)


def test_repeat_reuses_chunks_and_counts_parent_once(tmp_path, source):
    asset, calls, extract = source
    first = media.extract_media(tmp_path)
    second = media.extract_media(tmp_path)
    assert first["valid"] and second["valid"]
    assert calls == [(0, 20), (20, 25)] and second["cached_assets"] == 1
    assert first["payload_bytes"] == second["payload_bytes"]
    assert second["counts"]["source_assets"] == 1
    assert second["counts"]["retrieval_documents"] == 2
    assets, records = media.load_media_documents(tmp_path)
    assert all(row["document"]["asset_id"] == asset.id for row in records)


def test_interrupted_run_resumes_only_remaining_chunk(tmp_path, source, monkeypatch):
    asset, calls, extract = source

    def interrupt(source, destination, modality, start, end):
        if start == 20:
            raise KeyboardInterrupt()
        extract(source, destination, modality, start, end)

    monkeypatch.setattr(media, "extract_segment", interrupt)
    with pytest.raises(KeyboardInterrupt):
        media.extract_media(tmp_path)
    with pytest.raises(ValueError, match="incomplete"):
        media.load_media_documents(tmp_path)
    monkeypatch.setattr(media, "extract_segment", extract)
    assert media.extract_media(tmp_path)["valid"]
    assert calls == [(0, 20), (20, 25)]


def test_cached_payload_tampering_fails_closed(tmp_path, source):
    assert media.extract_media(tmp_path)["valid"]
    path = next((tmp_path / "cleaned/multimodal/content").glob("*.wav"))
    path.write_bytes(b"changed")
    with pytest.raises(ValueError):
        media.load_media_documents(tmp_path)
    assert not media.extract_media(tmp_path)["valid"]


@pytest.mark.parametrize("budget", ["byte", "document"])
def test_budget_failure_invalidates_prior_success(tmp_path, source, budget):
    assert media.extract_media(tmp_path)["valid"]
    limits = {"byte_budget": 1} if budget == "byte" else {"document_budget": 1}
    report = media.extract_media(tmp_path, **limits)
    assert not report["valid"]
    with pytest.raises(ValueError, match="incomplete"):
        media.load_media_documents(tmp_path)


def test_different_profile_reextracts(tmp_path, source):
    asset, calls, extract = source
    first = media.extract_media(tmp_path, segment_seconds=20)
    second = media.extract_media(tmp_path, segment_seconds=10)
    assert first["version"] != second["version"] and second["valid"]
    assert calls[-3:] == [(0, 10), (10, 20), (20, 25)]


def test_suffix_cannot_change_modality(tmp_path, source):
    asset, calls, extract = source
    media.extract_media(tmp_path)
    path = tmp_path / "cleaned/multimodal/media_documents.jsonl"
    records = [json.loads(line) for line in path.read_text().splitlines()]
    records[0]["suffix"] = ".mp4"
    with pytest.raises(ValueError, match="suffix"):
        media.validate_records(
            tmp_path, records, [asset], records[0]["document"]["extraction_version"]
        )


def test_video_extent_uses_visual_packets_not_audio_container(tmp_path, monkeypatch):
    calls = []

    def probe(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(
            stdout=json.dumps(
                {
                    "packets": [
                        {"pts_time": "0.003", "duration_time": "0.001"},
                        {"pts_time": "8.601", "duration_time": "0.001"},
                    ]
                }
            )
        )

    monkeypatch.setattr(media.subprocess, "run", probe)
    assert media.inspect_duration(tmp_path / "video.webm", "video") == pytest.approx(8.602)
    assert "v:0" in calls[0] and "format=duration" not in calls[0]
