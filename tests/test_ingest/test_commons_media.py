"""A reviewed source must pass terms, byte identity and decoding before publication."""

import json
from types import SimpleNamespace

import pytest
import requests
from filelock import FileLock, Timeout

from src.corpus.contracts import sha256
from src.ingest import commons_media as media


class Response:
    def __init__(self, content, status=200):
        self.content, self.status_code = content, status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def iter_content(self, size):
        yield self.content


class Session:
    def __init__(self, metadata, content=b"good"):
        self.headers = {}
        self.metadata, self.content = metadata, content
        self.downloads = 0

    def get(self, url, **kwargs):
        if url == media.API:
            return Response(json.dumps(self.metadata).encode())
        self.downloads += 1
        return Response(self.content)


@pytest.fixture
def source(monkeypatch):
    pin = media.MediaPin(
        title="File:Stadium.png",
        modality="image",
        sha256=sha256(b"good"),
        bytes=4,
        attribution="Author",
        license_name="CC BY 4.0",
        license_url="https://creativecommons.org/licenses/by/4.0/",
        event_date="2024-05-15",
    )
    metadata = {
        "query": {
            "pages": {
                "1": {
                    "title": pin.title,
                    "imageinfo": [
                        {
                            "url": "https://upload.wikimedia.org/wikipedia/commons/a/a1/Stadium.png?tracking=1",
                            "descriptionurl": "https://commons.wikimedia.org/wiki/File:Stadium.png",
                            "mime": "image/png",
                            "size": 4,
                            "extmetadata": {
                                "Artist": {"value": "<b>Author</b>"},
                                "LicenseShortName": {"value": pin.license_name},
                                "LicenseUrl": {"value": pin.license_url},
                                "DateTimeOriginal": {"value": "2024-05-15"},
                                "DateTime": {"value": "2024-05-16 10:00:00"},
                            },
                        }
                    ],
                }
            }
        }
    }
    monkeypatch.setattr(media, "decode_asset", lambda *args: {"decoded_seconds_limit": 2})
    monkeypatch.setattr(media, "load_pins", lambda: [pin])
    return pin, metadata


def test_repeat_collection_reuses_verified_bytes(tmp_path, source):
    pin, metadata = source
    session = Session(metadata)
    first = media.collect_media(tmp_path, pins=[pin], session=session)
    second = media.collect_media(tmp_path, pins=[pin], session=session)
    assert first["valid"] and second["valid"]
    assert session.downloads == 1 and second["assets"][0]["cached"]
    assert second["counts"]["source_assets"] == 1
    assert second["counts"]["retrieval_documents"] == 0
    assert media.load_verified_media(tmp_path)[0].scope == "background"


@pytest.mark.parametrize("content", [b"bad!", b"too long", b"short"])
def test_wrong_network_bytes_are_never_published(tmp_path, source, content):
    pin, metadata = source
    report = media.collect_media(tmp_path, pins=[pin], session=Session(metadata, content))
    assert not report["valid"] and len(report["failures"]) == 1
    assert not (tmp_path / "cleaned/multimodal/commons_assets.jsonl").exists()
    assert not list((tmp_path / "raw").rglob("*.blob")) if (tmp_path / "raw").exists() else True


def test_changed_terms_invalidate_old_success(tmp_path, source):
    pin, metadata = source
    media.collect_media(tmp_path, pins=[pin], session=Session(metadata))
    metadata["query"]["pages"]["1"]["imageinfo"][0]["extmetadata"]["Artist"]["value"] = "Other"
    report = media.collect_media(tmp_path, pins=[pin], session=Session(metadata))
    assert not report["valid"]
    with pytest.raises(ValueError, match="not passed"):
        media.load_verified_media(tmp_path)


def test_corrupt_cache_is_rejected_without_redownload(tmp_path, source):
    pin, metadata = source
    session = Session(metadata)
    media.collect_media(tmp_path, pins=[pin], session=session)
    (tmp_path / "raw/multimodal/commons" / (pin.sha256 + ".blob")).write_bytes(b"bad!")
    assert not media.collect_media(tmp_path, pins=[pin], session=session)["valid"]
    assert session.downloads == 1


@pytest.mark.parametrize(
    "relative",
    [
        "cleaned/multimodal/commons_assets.jsonl",
        "raw/multimodal/commons/{sha}.blob",
        "raw/multimodal/metadata/{sha}.json",
    ],
)
def test_accepted_evidence_tampering_fails(tmp_path, source, relative):
    pin, metadata = source
    media.collect_media(tmp_path, pins=[pin], session=Session(metadata))
    path = tmp_path / relative.format(sha=pin.sha256)
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError):
        media.load_verified_media(tmp_path)


def test_decode_failure_is_not_accepted(tmp_path, source, monkeypatch):
    pin, metadata = source

    def fail(*args):
        raise ValueError("Invalid media streams")

    monkeypatch.setattr(media, "decode_asset", fail)
    assert not media.collect_media(tmp_path, pins=[pin], session=Session(metadata))["valid"]


@pytest.mark.parametrize(
    "url",
    [
        "https://localhost/media",
        "http://upload.wikimedia.org/file",
        "https://upload.wikimedia.org.evil.example/file",
        "https://user:secret@upload.wikimedia.org/file",
    ],
)
def test_download_host_boundary(url):
    with pytest.raises(ValueError):
        media.canonical_url(url, "upload.wikimedia.org")


def test_budget_is_checked_before_network(tmp_path, source):
    pin, metadata = source
    with pytest.raises(ValueError, match="budget"):
        media.collect_media(tmp_path, pins=[pin], byte_budget=3, session=Session(metadata))


def test_collector_is_serialized(tmp_path, source):
    pin, metadata = source
    with FileLock(str(tmp_path / "commons.lock")):
        with pytest.raises(Timeout):
            media.collect_media(tmp_path, pins=[pin], session=Session(metadata))


def test_retries_transient_errors_only(monkeypatch):
    monkeypatch.setattr(media.time, "sleep", lambda seconds: None)
    attempts = []

    def transient():
        attempts.append(1)
        if len(attempts) < 3:
            raise requests.HTTPError(response=SimpleNamespace(status_code=503))
        return "ok"

    assert media.retry_network(transient) == "ok" and len(attempts) == 3
    attempts.clear()

    def permanent():
        attempts.append(1)
        raise requests.HTTPError(response=SimpleNamespace(status_code=404))

    with pytest.raises(requests.HTTPError):
        media.retry_network(permanent)
    assert len(attempts) == 1


def test_interrupted_rerun_never_leaves_old_ready_report(tmp_path, source, monkeypatch):
    pin, metadata = source
    media.collect_media(tmp_path, pins=[pin], session=Session(metadata))

    def interrupt(*args):
        raise KeyboardInterrupt()

    monkeypatch.setattr(media, "fetch_metadata", interrupt)
    with pytest.raises(KeyboardInterrupt):
        media.collect_media(tmp_path, pins=[pin], session=Session(metadata))
    with pytest.raises(ValueError, match="not passed"):
        media.load_verified_media(tmp_path)
    assert not (tmp_path / "cleaned/multimodal/commons_assets.jsonl").exists()


@pytest.mark.parametrize(
    "modality,streams",
    [
        ("audio", [{"codec_type": "video", "codec_name": "vp8"}]),
        ("video", [{"codec_type": "audio", "codec_name": "vorbis"}]),
        ("image", [{"codec_type": "video", "codec_name": "vp8"}]),
    ],
)
def test_real_stream_contract_rejects_wrong_modality(tmp_path, modality, streams, monkeypatch):
    from src.corpus.contracts import Asset, content_id

    checksum = sha256(b"sample")
    asset = Asset(
        id=content_id("asset", checksum),
        sha256=checksum,
        modality=modality,
        mime_type={"audio": "audio/ogg", "video": "video/webm", "image": "image/png"}[modality],
        bytes=6,
        source_url="https://upload.wikimedia.org/sample",
        reference_url="https://commons.wikimedia.org/sample",
        publisher="Commons",
        attribution="Author",
        license_name="CC BY 4.0",
        license_url="https://creativecommons.org/licenses/by/4.0/",
        scope="background",
    )
    monkeypatch.setattr(
        media.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps({"streams": streams})),
    )
    with pytest.raises(ValueError):
        media.decode_asset(asset, tmp_path / "sample")
