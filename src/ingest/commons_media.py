"""Bounded collection of reviewed Wikimedia media with pinned content and terms."""

import json
import subprocess
import time
from datetime import date
from importlib.resources import files
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from filelock import FileLock
from pydantic import BaseModel, ConfigDict, Field

from src.corpus.contracts import Asset, Modality, content_id, count_corpus, sha256
from src.ingest.source_download import atomic_write

API = "https://commons.wikimedia.org/w/api.php"
MAX_FILE_BYTES = 20_000_000


class MediaPin(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    title: str = Field(pattern=r"^File:.+")
    modality: Modality
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    bytes: int = Field(gt=0, le=MAX_FILE_BYTES)
    attribution: str = Field(min_length=1)
    license_name: str = Field(min_length=1)
    license_url: str
    event_date: date | None = None


def load_pins() -> list[MediaPin]:
    rows = json.loads(files("config").joinpath("commons_media.json").read_text())
    pins = [MediaPin.model_validate(row) for row in rows]
    if len({row.title for row in pins}) != len(pins):
        raise ValueError("Duplicate Commons source titles")
    return pins


def canonical_url(value: str, host: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or parsed.hostname != host or parsed.username or parsed.password:
        raise ValueError("Untrusted Commons source URL")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def license_url(value: str) -> str:
    # Commons legacy CC links use HTTP; canonicalize only this known terms host.
    if value.startswith("http://creativecommons.org/"):
        value = "https://" + value[len("http://") :]
    return canonical_url(value, "creativecommons.org")


def metadata_asset(pin: MediaPin, metadata: dict) -> Asset:
    info = metadata["query"]["pages"]
    pages = list(info.values())
    if len(pages) != 1 or pages[0].get("title") != pin.title:
        raise ValueError("Commons title resolution differs from reviewed source")
    record = pages[0]["imageinfo"][0]
    terms = record["extmetadata"]
    author = BeautifulSoup(terms["Artist"]["value"], "html.parser").get_text(" ", strip=True)
    name = terms["LicenseShortName"]["value"]
    url = license_url(terms["LicenseUrl"]["value"])
    event = terms.get("DateTimeOriginal", {}).get("value")
    try:
        event_date = date.fromisoformat(event) if event else None
    except ValueError:
        event_date = None
    if (author, name, url, event_date, record["size"]) != (
        pin.attribution,
        pin.license_name,
        pin.license_url,
        pin.event_date,
        pin.bytes,
    ):
        raise ValueError("Commons attribution, terms, date or size changed; review the source pin")
    mime = record["mime"]
    if mime == "application/ogg":
        mime = "audio/ogg" if pin.modality == "audio" else "video/ogg"
    published = terms.get("DateTime", {}).get("value", "")[:10]
    try:
        published_at = date.fromisoformat(published)
    except ValueError:
        published_at = None
    return Asset(
        id=content_id("asset", pin.sha256),
        sha256=pin.sha256,
        modality=pin.modality,
        mime_type=mime,
        bytes=pin.bytes,
        source_url=canonical_url(record["url"], "upload.wikimedia.org"),
        reference_url=canonical_url(record["descriptionurl"], "commons.wikimedia.org"),
        publisher="Wikimedia Commons",
        attribution=author,
        license_name=name,
        license_url=url,
        published_at=published_at,
        event_date=event_date,
        scope="background",
    )


def fetch_metadata(session, pin: MediaPin) -> dict:
    with session.get(
        API,
        params={
            "action": "query",
            "format": "json",
            "titles": pin.title,
            "prop": "imageinfo",
            "iiprop": "url|size|mime|extmetadata",
        },
        timeout=(10, 30),
        allow_redirects=False,
        stream=True,
    ) as response:
        response.raise_for_status()
        if response.status_code != 200:
            raise ValueError("Commons metadata redirects are unsupported")
        content = bytearray()
        for chunk in response.iter_content(65536):
            content.extend(chunk)
            if len(content) > 2_000_000:
                raise ValueError("Commons metadata exceeds size limit")
        return json.loads(content)


def download_asset(session, asset: Asset, path: Path) -> bool:
    """Return cache-hit status; modified cache or network bytes fail closed."""
    if path.exists():
        if path.stat().st_size != asset.bytes:
            raise ValueError("Cached media size differs from reviewed source")
        content = path.read_bytes()
        if len(content) != asset.bytes or sha256(content) != asset.sha256:
            raise ValueError("Cached media differs from source checksum")
        return True
    with session.get(
        asset.source_url, timeout=(10, 30), stream=True, allow_redirects=False
    ) as response:
        response.raise_for_status()
        if response.status_code != 200:
            raise ValueError("Media redirects are unsupported")
        content = bytearray()
        for chunk in response.iter_content(65536):
            content.extend(chunk)
            if len(content) > asset.bytes or len(content) > MAX_FILE_BYTES:
                raise ValueError("Media download exceeds reviewed byte limit")
    if len(content) != asset.bytes or sha256(content) != asset.sha256:
        raise ValueError("Downloaded media differs from source checksum")
    atomic_write(path, content)
    return False


def decode_asset(asset: Asset, path: Path) -> dict:
    """Inspect real stream types and decode a bounded sample without providers."""
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-of",
            "json",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    probe = json.loads(result.stdout)
    streams = probe["streams"]
    kinds = {stream["codec_type"] for stream in streams}
    if asset.modality == "audio" and ("audio" not in kinds or "video" in kinds):
        raise ValueError("Audio source does not contain only audio media")
    if asset.modality == "video" and "video" not in kinds:
        raise ValueError("Video source has no video stream")
    if asset.modality == "image" and (
        "audio" in kinds
        or not streams
        or any(
            stream.get("codec_name") not in {"mjpeg", "png", "webp", "svg"} for stream in streams
        )
    ):
        raise ValueError("Image source has incompatible streams")
    if asset.modality == "text":
        raise ValueError("Commons media collector does not accept text sources")
    subprocess.run(
        [
            "ffmpeg",
            "-nostdin",
            "-v",
            "error",
            "-i",
            str(path),
            "-t",
            "2",
            "-f",
            "null",
            "-",
        ],
        capture_output=True,
        check=True,
        timeout=30,
    )
    return {
        "streams": [{"type": x["codec_type"], "codec": x["codec_name"]} for x in streams],
        "duration": probe.get("format", {}).get("duration"),
        "decoded_seconds_limit": 2,
    }


def retry_network(operation):
    """Retry transient network failures only; never retry validation failures."""
    for attempt in range(3):
        try:
            return operation()
        except requests.RequestException as exc:
            status = exc.response.status_code if exc.response is not None else None
            if attempt == 2 or (status is not None and status < 500 and status != 429):
                raise
            time.sleep(2**attempt)
    raise AssertionError("Unreachable retry state")


def collect_media(output: Path, *, pins=None, byte_budget=50_000_000, session=None) -> dict:
    pins = load_pins() if pins is None else pins
    if (
        not pins
        or len(pins) > 50
        or sum(pin.bytes for pin in pins) > byte_budget
        or len({p.title for p in pins}) != len(pins)
        or len({p.sha256 for p in pins}) != len(pins)
    ):
        raise ValueError("Empty source selection or collection exceeds asset/byte budget")
    output.mkdir(parents=True, exist_ok=True)
    owned = session is None
    session = session or requests.Session()
    session.headers["User-Agent"] = "PLKnowledgeEngine/0.1 (reviewed public media collection)"
    report_path = output / "reports/multimodal/commons.json"
    report = {
        "valid": False,
        "source": "Wikimedia Commons",
        "assets": [],
        "failures": [],
        "requested_assets": len(pins),
        "byte_budget": byte_budget,
    }
    try:
        with FileLock(str(output / "commons.lock"), timeout=0):
            manifest = output / "cleaned/multimodal/commons_assets.jsonl"
            atomic_write(report_path, (json.dumps(report, indent=2) + "\n").encode())
            manifest.unlink(missing_ok=True)
            assets = []
            for pin in pins:
                try:
                    metadata = retry_network(lambda: fetch_metadata(session, pin))
                    asset = metadata_asset(pin, metadata)
                    path = output / "raw/multimodal/commons" / (asset.sha256 + ".blob")
                    cached = retry_network(lambda: download_asset(session, asset, path))
                    decoded = decode_asset(asset, path)
                    snapshot = output / "raw/multimodal/metadata" / (asset.sha256 + ".json")
                    atomic_write(snapshot, (json.dumps(metadata, sort_keys=True) + "\n").encode())
                    assets.append(asset)
                    report["assets"].append(
                        {
                            "id": asset.id,
                            "title": pin.title,
                            "cached": cached,
                            "decoding": decoded,
                            "metadata_sha256": sha256(snapshot.read_bytes()),
                        }
                    )
                except (
                    ValueError,
                    KeyError,
                    requests.RequestException,
                    OSError,
                    subprocess.SubprocessError,
                ) as exc:
                    report["failures"].append(
                        {"title": pin.title, "error": type(exc).__name__, "detail": str(exc)[:500]}
                    )
            report["counts"] = count_corpus(assets, [])
            report["valid"] = not report["failures"] and len(assets) == len(pins)
            # A failed run replaces prior success; partial source manifests never become accepted.
            if report["valid"]:
                atomic_write(manifest, "".join(a.model_dump_json() + "\n" for a in assets).encode())
                report["manifest_sha256"] = sha256(manifest.read_bytes())
            else:
                manifest.unlink(missing_ok=True)
            atomic_write(report_path, (json.dumps(report, indent=2) + "\n").encode())
    finally:
        if owned:
            session.close()
    return report


def load_verified_media(output: Path) -> list[Asset]:
    """Require current accepted manifests, source snapshots and verified media bytes."""
    report = json.loads((output / "reports/multimodal/commons.json").read_text())
    manifest = output / "cleaned/multimodal/commons_assets.jsonl"
    if not report.get("valid") or sha256(manifest.read_bytes()) != report["manifest_sha256"]:
        raise ValueError("Media collection has not passed or its manifest changed")
    assets = [Asset.model_validate_json(line) for line in manifest.read_text().splitlines()]
    records = {row["id"]: row for row in report["assets"]}
    if set(records) != {asset.id for asset in assets}:
        raise ValueError("Media report and manifest identities differ")
    pins = {pin.title: pin for pin in load_pins()}
    for asset in assets:
        record = records[asset.id]
        snapshot = output / "raw/multimodal/metadata" / (asset.sha256 + ".json")
        metadata_bytes = snapshot.read_bytes()
        if sha256(metadata_bytes) != record["metadata_sha256"]:
            raise ValueError("Media provenance snapshot changed")
        if metadata_asset(pins[record["title"]], json.loads(metadata_bytes)) != asset:
            raise ValueError("Media asset differs from pinned provenance")
        media_path = output / "raw/multimodal/commons" / (asset.sha256 + ".blob")
        if media_path.stat().st_size != asset.bytes:
            raise ValueError("Accepted media size changed")
        content = media_path.read_bytes()
        if len(content) != asset.bytes or sha256(content) != asset.sha256:
            raise ValueError("Accepted media bytes changed")
    if count_corpus(assets, []) != report["counts"]:
        raise ValueError("Media coverage report differs from accepted assets")
    return assets
