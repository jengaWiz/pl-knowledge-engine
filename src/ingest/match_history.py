"""Separate, pinned historical seasons for retrieval and later temporal evaluation."""

import json
from dataclasses import asdict
from importlib.resources import files
from pathlib import Path

import requests
from filelock import FileLock

from config.season import season_bounds
from config.sources import SourceSpec, inspect_source
from src.corpus.contracts import Asset, Document, content_id, count_corpus, document_id, sha256
from src.corpus.statistical_text import encoded, write_lines
from src.corpus.text_rendering import render_match_text
from src.ingest.historical_matches import normalize_matches, validate_coverage
from src.ingest.source_download import atomic_write, download_source

SCHEMA = "historical-match-text-v1"


def load_history_sources() -> list[SourceSpec]:
    records = json.loads(files("config").joinpath("historical_sources.json").read_text())
    sources = [SourceSpec(**record) for record in records]
    if not sources or len({source.season for source in sources}) != len(sources):
        raise ValueError("Historical seasons must be nonempty and unique")
    for source in sources:
        season_bounds(source.season)
        year = int(source.season[:4])
        code = f"{year % 100:02d}{(year + 1) % 100:02d}"
        if source.id != "matches" or source.url != (
            f"https://www.football-data.co.uk/mmz4281/{code}/E0.csv"
        ):
            raise ValueError("Historical source must identify its reviewed PL season CSV")
    return sorted(sources, key=lambda source: source.season)


def raw_path(output: Path, season: str):
    return output / "raw/history" / season / "matches.csv"


def paths(output: Path):
    return (
        output / "cleaned/history/assets.jsonl",
        output / "cleaned/history/documents.jsonl",
        output / "reports/history/corpus.json",
    )


def build_history(output: Path, sources: list[SourceSpec]):
    profile = {"schema": SCHEMA, "sources": [asdict(source) for source in sources]}
    version = SCHEMA + ":" + sha256(encoded(profile))
    assets, documents, normalized, coverage = [], [], {}, {}
    for source in sources:
        content = raw_path(output, source.season).read_bytes()
        inspect_source(source, content)
        matches = normalize_matches(content, source.season, source.id)
        check = validate_coverage(matches)
        if not check["valid"]:
            raise ValueError(f"{source.season}: historical match completeness gate failed")
        coverage[source.season] = check
        original = Asset(
            id=content_id("asset", source.sha256),
            sha256=source.sha256,
            modality="text",
            mime_type="text/csv",
            bytes=len(content),
            source_url=source.url,
            reference_url=source.reference_url,
            publisher=source.publisher,
            attribution=source.publisher,
            license_name=source.reuse_terms,
            license_url=source.reference_url,
            scope="background",
        )
        assets.append(original)
        for match in matches:
            match.update(
                source_url=source.url, source_sha256=source.sha256, source_revision=source.revision
            )
            text = render_match_text(match)
            checksum = sha256(text.encode())
            derived = Asset(
                **{
                    **original.model_dump(),
                    "id": content_id("asset", checksum),
                    "sha256": checksum,
                    "mime_type": "text/plain",
                    "bytes": len(text.encode()),
                    "parent_asset_id": original.id,
                    "scope": "seasonal",
                    "season": source.season,
                    "event_date": match["date"],
                    "entity_ids": (match["id"],),
                }
            )
            assets.append(derived)
            doc = Document(
                id=document_id(derived.id, "text", 0, len(text), version, checksum),
                asset_id=derived.id,
                modality="text",
                sha256=checksum,
                extraction_version=version,
                start=0,
                end=len(text),
                coordinate="characters",
            )
            documents.append(
                {
                    "document": doc.model_dump(),
                    "text": text,
                    "kind": "match",
                    "season": source.season,
                    "event_date": match["date"],
                    "record_ids": [match["id"]],
                    "source_refs": [
                        {
                            "id": source.id,
                            "season": source.season,
                            "asset_id": original.id,
                            "url": source.url,
                            "sha256": source.sha256,
                            "revision": source.revision,
                            "row": match["source_row"],
                        }
                    ],
                }
            )
        normalized[source.season] = matches
    summary = {
        "schema": SCHEMA,
        "profile": profile,
        "coverage": coverage,
        "counts": count_corpus(assets, [Document.model_validate(r["document"]) for r in documents]),
        "payload_bytes": sum(len(row["text"].encode()) for row in documents),
    }
    return assets, documents, normalized, summary


def collect_history(output: Path, *, document_budget=2000, byte_budget=2_000_000):
    if document_budget <= 0 or byte_budget <= 0:
        raise ValueError("Historical extraction budgets must be positive")
    output.mkdir(parents=True, exist_ok=True)
    assets_path, docs_path, report_path = paths(output)
    with FileLock(str(output / "history.lock"), timeout=0):
        report = {"valid": False, "schema": SCHEMA, "cached_seasons": []}
        atomic_write(report_path, encoded(report))
        assets_path.unlink(missing_ok=True)
        docs_path.unlink(missing_ok=True)
        try:
            sources = load_history_sources()
            for source in sources:
                _, cached = download_source(source, raw_path(output, source.season))
                if cached:
                    report["cached_seasons"].append(source.season)
            assets, records, normalized, summary = build_history(output, sources)
            if len(records) > document_budget or summary["payload_bytes"] > byte_budget:
                raise ValueError("Historical text exceeds the extraction budget")
            artifacts = {}
            for season, matches in normalized.items():
                path = output / "cleaned/history" / season / "matches.jsonl"
                write_lines(path, matches)
                artifacts[season] = sha256(path.read_bytes())
            write_lines(assets_path, [asset.model_dump(mode="json") for asset in assets])
            write_lines(docs_path, records)
            report.update(
                **summary,
                valid=True,
                normalized_sha256=artifacts,
                assets_sha256=sha256(assets_path.read_bytes()),
                documents_sha256=sha256(docs_path.read_bytes()),
            )
        except (ValueError, KeyError, TypeError, OSError, requests.RequestException) as exc:
            assets_path.unlink(missing_ok=True)
            docs_path.unlink(missing_ok=True)
            report.update(error=type(exc).__name__, detail=str(exc)[:500])
        atomic_write(report_path, encoded(report))
        return report


def load_history_documents(output: Path):
    with FileLock(str(output / "history.lock"), timeout=0):
        assets_path, docs_path, report_path = paths(output)
        report = json.loads(report_path.read_text())
        if not report.get("valid") or report.get("schema") != SCHEMA:
            raise ValueError("Historical corpus collection is incomplete")
        if (
            sha256(assets_path.read_bytes()) != report["assets_sha256"]
            or sha256(docs_path.read_bytes()) != report["documents_sha256"]
        ):
            raise ValueError("Historical corpus manifest changed")
        assets, records, normalized, summary = build_history(output, load_history_sources())
        for season, matches in normalized.items():
            content = (output / "cleaned/history" / season / "matches.jsonl").read_bytes()
            if sha256(content) != report["normalized_sha256"][season] or content != b"".join(
                encoded(row) for row in matches
            ):
                raise ValueError("Normalized history no longer matches its raw source")
        if (
            assets_path.read_bytes() != b"".join(encoded(a.model_dump(mode="json")) for a in assets)
            or docs_path.read_bytes() != b"".join(encoded(row) for row in records)
            or any(report.get(key) != value for key, value in summary.items())
        ):
            raise ValueError("Historical corpus no longer matches its source evidence")
        return assets, records, summary
