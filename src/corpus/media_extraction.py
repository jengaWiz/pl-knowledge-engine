"""Extract bounded, checksummed retrieval units from accepted public media."""

import json
import math
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

from filelock import FileLock

from src.corpus.contracts import Document, count_corpus, document_id, sha256
from src.ingest.commons_media import load_verified_media
from src.ingest.source_download import atomic_write

SCHEMA = "media-extraction-v1"
SUFFIX = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}


def ranges(duration: float, seconds: int) -> list[tuple[float, float]]:
    if not math.isfinite(duration) or duration <= 0 or not 5 <= seconds <= 60:
        raise ValueError("Media duration and segment length must be bounded and positive")
    return [
        (float(start), min(float(start + seconds), duration))
        for start in range(0, math.ceil(duration), seconds)
    ]


def inspect_duration(path: Path, modality: str | None = None) -> float:
    if modality == "video":
        # Container duration can include an audio tail with no visual evidence.
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "packet=pts_time,duration_time",
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        packets = json.loads(result.stdout)["packets"]
        duration = max(
            float(packet["pts_time"]) + float(packet.get("duration_time", 0)) for packet in packets
        )
        if not math.isfinite(duration) or duration <= 0:
            raise ValueError("Video timeline must be finite and positive")
        return duration
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "json",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    duration = float(json.loads(result.stdout)["format"]["duration"])
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError("Source duration must be finite and positive")
    return duration


def extract_segment(source: Path, destination: Path, modality: str, start: float, end: float):
    args = [
        "ffmpeg",
        "-nostdin",
        "-v",
        "error",
        "-ss",
        str(start),
        "-i",
        str(source),
        "-t",
        str(end - start),
    ]
    if modality == "audio":
        args += ["-vn", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le"]
    elif modality == "video":
        args += [
            "-an",
            "-vf",
            "fps=2,scale=min(480\\,iw):-2",
            "-c:v",
            "libx264",
            "-preset",
            "fast",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
        ]
    else:
        raise ValueError("Unsupported segmented modality")
    args += ["-map_metadata", "-1", "-y", str(destination)]
    subprocess.run(args, capture_output=True, check=True, timeout=120)
    decoded = inspect_duration(destination)
    if abs(decoded - (end - start)) > (0.6 if modality == "video" else 0.05):
        raise ValueError("Extracted duration differs from the requested source range")
    subprocess.run(
        ["ffmpeg", "-nostdin", "-v", "error", "-i", str(destination), "-f", "null", "-"],
        capture_output=True,
        check=True,
        timeout=60,
    )


def validate_records(output: Path, records: list[dict], assets, version: str) -> list[Document]:
    by_id = {asset.id: asset for asset in assets}
    documents = []
    for record in records:
        doc = Document.model_validate(record["document"])
        if doc.asset_id not in by_id or doc.extraction_version != version:
            raise ValueError("Extracted document parent or version changed")
        suffix = record["suffix"]
        expected_suffix = (
            SUFFIX.get(by_id[doc.asset_id].mime_type)
            if doc.modality == "image"
            else (".wav" if doc.modality == "audio" else ".mp4")
        )
        if suffix != expected_suffix:
            raise ValueError("Unsupported payload suffix")
        payload = output / "cleaned/multimodal/content" / (doc.sha256 + suffix)
        if payload.stat().st_size != record["bytes"] or sha256(payload.read_bytes()) != doc.sha256:
            raise ValueError("Extracted payload differs from its accepted checksum")
        documents.append(doc)
    count_corpus(assets, documents)
    return documents


def extract_media(
    output: Path, *, segment_seconds=20, byte_budget=100_000_000, document_budget=500
) -> dict:
    if not 5 <= segment_seconds <= 60 or byte_budget <= 0 or document_budget <= 0:
        raise ValueError("Invalid extraction limits")
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "reports/multimodal/extraction.json"
    manifest = output / "cleaned/multimodal/media_documents.jsonl"
    with (
        FileLock(str(output / "commons.lock"), timeout=0),
        FileLock(str(output / "media-extraction.lock"), timeout=0),
    ):
        assets = load_verified_media(output)
        tool = subprocess.run(
            ["ffmpeg", "-version"], capture_output=True, text=True, check=True, timeout=10
        ).stdout.splitlines()[0]
        profile = {
            "schema": SCHEMA,
            "seconds": segment_seconds,
            "tool": tool,
            "sources": [asset.model_dump(mode="json") for asset in assets],
        }
        version = SCHEMA + ":" + sha256(json.dumps(profile, sort_keys=True).encode())
        report = {
            "valid": False,
            "version": version,
            "profile": profile,
            "byte_budget": byte_budget,
            "document_budget": document_budget,
        }
        atomic_write(report_path, (json.dumps(report, indent=2) + "\n").encode())
        manifest.unlink(missing_ok=True)
        records, used_bytes, cache_hits = [], 0, 0
        try:
            for asset in assets:
                source = output / "raw/multimodal/commons" / (asset.sha256 + ".blob")
                spans = (
                    [(0.0, 1.0)]
                    if asset.modality == "image"
                    else ranges(inspect_duration(source, asset.modality), segment_seconds)
                )
                if len(records) + len(spans) > document_budget:
                    raise ValueError("Extraction exceeds the document budget")
                checkpoint = (
                    output / "checkpoints/media" / (sha256((asset.id + version).encode()) + ".json")
                )
                batch = []
                if checkpoint.exists():
                    cached = json.loads(checkpoint.read_text())
                    docs = validate_records(output, cached, [asset], version)
                    if [(doc.start, doc.end) for doc in docs] != spans[: len(docs)]:
                        raise ValueError("Cached extraction ranges differ from the source")
                    batch = cached
                    used_bytes += sum(row["bytes"] for row in batch)
                    cache_hits += 1
                if used_bytes > byte_budget:
                    raise ValueError("Cached extraction exceeds the payload byte budget")
                for start, end in spans[len(batch) :]:
                    suffix = (
                        SUFFIX[asset.mime_type]
                        if asset.modality == "image"
                        else (".wav" if asset.modality == "audio" else ".mp4")
                    )
                    with TemporaryDirectory(prefix="pl-media-") as folder:
                        if asset.modality == "image":
                            content = source.read_bytes()
                        else:
                            destination = Path(folder) / ("segment" + suffix)
                            extract_segment(source, destination, asset.modality, start, end)
                            if destination.stat().st_size > byte_budget - used_bytes:
                                raise ValueError("Extraction exceeds the payload byte budget")
                            content = destination.read_bytes()
                        if not content or len(content) + used_bytes > byte_budget:
                            raise ValueError("Empty payload or extraction byte budget exceeded")
                        checksum = sha256(content)
                        doc = Document(
                            id=document_id(asset.id, asset.modality, start, end, version, checksum),
                            asset_id=asset.id,
                            modality=asset.modality,
                            sha256=checksum,
                            extraction_version=version,
                            start=start,
                            end=end,
                            coordinate="whole_image" if asset.modality == "image" else "seconds",
                        )
                        atomic_write(
                            output / "cleaned/multimodal/content" / (checksum + suffix), content
                        )
                        batch.append(
                            {
                                "document": doc.model_dump(),
                                "suffix": suffix,
                                "bytes": len(content),
                            }
                        )
                        used_bytes += len(content)
                    atomic_write(checkpoint, (json.dumps(batch) + "\n").encode())
                if used_bytes > byte_budget:
                    raise ValueError("Cached extraction exceeds the payload byte budget")
                records.extend(batch)
            documents = validate_records(output, records, assets, version)
            report.update(
                valid=True,
                counts=count_corpus(assets, documents),
                payload_bytes=used_bytes,
                cached_assets=cache_hits,
            )
            atomic_write(manifest, "".join(json.dumps(row) + "\n" for row in records).encode())
            report["manifest_sha256"] = sha256(manifest.read_bytes())
        except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
            manifest.unlink(missing_ok=True)
            report.update(error=type(exc).__name__, detail=str(exc)[:500])
        atomic_write(report_path, (json.dumps(report, indent=2) + "\n").encode())
        return report


def load_media_documents(output: Path) -> tuple[list, list]:
    with (
        FileLock(str(output / "commons.lock"), timeout=0),
        FileLock(str(output / "media-extraction.lock"), timeout=0),
    ):
        assets = load_verified_media(output)
        report = json.loads((output / "reports/multimodal/extraction.json").read_text())
        manifest = output / "cleaned/multimodal/media_documents.jsonl"
        if not report.get("valid") or sha256(manifest.read_bytes()) != report["manifest_sha256"]:
            raise ValueError("Media extraction is incomplete or changed")
        records = [json.loads(line) for line in manifest.read_text().splitlines()]
        profile = report["profile"]
        if (
            profile["schema"] != SCHEMA
            or profile["sources"] != [asset.model_dump(mode="json") for asset in assets]
            or report["version"]
            != SCHEMA + ":" + sha256(json.dumps(profile, sort_keys=True).encode())
        ):
            raise ValueError("Extraction profile or parent sources changed")
        docs = validate_records(output, records, assets, report["version"])
        for asset in assets:
            source = output / "raw/multimodal/commons" / (asset.sha256 + ".blob")
            expected = (
                [(0.0, 1.0)]
                if asset.modality == "image"
                else ranges(inspect_duration(source, asset.modality), profile["seconds"])
            )
            actual = [(doc.start, doc.end) for doc in docs if doc.asset_id == asset.id]
            if actual != expected:
                raise ValueError("Extraction source coverage is incomplete or changed")
        if count_corpus(assets, docs) != report["counts"]:
            raise ValueError("Extracted document counts changed")
        return assets, records
