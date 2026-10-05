"""Combine accepted text/media without inflating counts or implying live indexing."""

import json
import subprocess
from collections import Counter
from pathlib import Path

from filelock import FileLock

from src.corpus.contracts import Document, count_corpus, sha256
from src.corpus.media_extraction import load_media_documents
from src.corpus.statistical_text import (
    encoded,
    load_text_documents,
    write_lines,
)
from src.corpus.statistical_text import (
    paths as text_paths,
)
from src.ingest.source_download import atomic_write

SCHEMA = "combined-corpus-v1"


def paths(output: Path, season: str):
    base = output / "cleaned/multimodal" / season
    return (
        base / "corpus_assets.jsonl",
        base / "corpus_documents.jsonl",
        (output / "reports/multimodal" / season / "corpus.json"),
    )


def input_snapshot(output: Path, season: str) -> dict:
    files = [
        *text_paths(output, season),
        output / "cleaned/multimodal/commons_assets.jsonl",
        output / "cleaned/multimodal/media_documents.jsonl",
        output / "reports/multimodal/commons.json",
        output / "reports/multimodal/extraction.json",
    ]
    return {str(path.relative_to(output)): sha256(path.read_bytes()) for path in files}


def combine(assets_groups, record_groups):
    """Shared identities can be reused only when the full contracts agree."""
    assets, records = {}, {}
    for group in assets_groups:
        for asset in group:
            if asset.id in assets and assets[asset.id] != asset:
                raise ValueError("Shared asset identity has conflicting provenance")
            assets[asset.id] = asset
    for stage, group in record_groups:
        for row in group:
            doc = Document.model_validate(row["document"])
            record = {"stage": stage, **row}
            if doc.id in records and records[doc.id] != record:
                raise ValueError("Shared document identity has conflicting evidence")
            records[doc.id] = record
    ordered_assets = [assets[key] for key in sorted(assets)]
    ordered_records = [records[key] for key in sorted(records)]
    count_corpus(
        ordered_assets, [Document.model_validate(row["document"]) for row in ordered_records]
    )
    return ordered_assets, ordered_records


def build_corpus(output: Path, season: str):
    before = input_snapshot(output, season)
    text_assets, text_records = load_text_documents(output, season)
    media_assets, media_records = load_media_documents(output)
    if input_snapshot(output, season) != before:
        raise ValueError("Input manifests changed during combined verification; retry")
    assets, records = combine(
        [text_assets, media_assets], [("statistical_text", text_records), ("media", media_records)]
    )
    docs = [Document.model_validate(row["document"]) for row in records]
    counts = count_corpus(assets, docs)
    by_id = {asset.id: asset for asset in assets}
    unique_payloads = {}
    for row in records:
        doc = row["document"]
        size = len(row["text"].encode()) if row["stage"] == "statistical_text" else row["bytes"]
        key = (doc["modality"], doc["sha256"])
        if key in unique_payloads and unique_payloads[key] != size:
            raise ValueError("Shared document content has inconsistent byte size")
        unique_payloads[key] = size
    dates = [
        by_id[doc.asset_id].event_date.isoformat()
        for doc in docs
        if by_id[doc.asset_id].scope == "seasonal"
    ]
    summary = {
        "schema": SCHEMA,
        "season": season,
        "input_sha256": before,
        "counts": counts,
        "payload_bytes": sum(unique_payloads.values()),
        "document_records": len(records),
        "document_records_by_scope": dict(Counter(by_id[doc.asset_id].scope for doc in docs)),
        "seasonal_date_range": [min(dates), max(dates)] if dates else None,
        "status": "prepared_target_met"
        if counts["multimodal_target_met"]
        else "prepared_below_target",
        "documents_short_of_target": max(0, 2500 - counts["retrieval_documents"]),
        "embeddings_created_by_stage": 0,
    }
    return assets, records, summary


def accept_corpus(output: Path, season: str, *, document_budget=5000, byte_budget=150_000_000):
    if document_budget <= 0 or byte_budget <= 0:
        raise ValueError("Combined corpus budgets must be positive")
    assets_path, docs_path, report_path = paths(output, season)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(report_path.parent / "corpus.lock"), timeout=0):
        report = {"valid": False, "schema": SCHEMA, "season": season}
        atomic_write(report_path, encoded(report))
        assets_path.unlink(missing_ok=True)
        docs_path.unlink(missing_ok=True)
        try:
            assets, records, summary = build_corpus(output, season)
            if len(records) > document_budget or summary["payload_bytes"] > byte_budget:
                raise ValueError("Combined corpus exceeds the acceptance budget")
            write_lines(assets_path, [asset.model_dump(mode="json") for asset in assets])
            write_lines(docs_path, records)
            report.update(
                **summary,
                valid=True,
                assets_sha256=sha256(assets_path.read_bytes()),
                documents_sha256=sha256(docs_path.read_bytes()),
            )
        except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as exc:
            assets_path.unlink(missing_ok=True)
            docs_path.unlink(missing_ok=True)
            report.update(error=type(exc).__name__, detail=str(exc)[:500])
        atomic_write(report_path, encoded(report))
        return report


def load_corpus(output: Path, season: str):
    assets_path, docs_path, report_path = paths(output, season)
    with FileLock(str(report_path.parent / "corpus.lock"), timeout=0):
        report = json.loads(report_path.read_text())
        if (
            not report.get("valid")
            or report.get("schema") != SCHEMA
            or report.get("season") != season
        ):
            raise ValueError("Combined corpus acceptance is incomplete")
        if sha256(assets_path.read_bytes()) != report["assets_sha256"] or (
            sha256(docs_path.read_bytes()) != report["documents_sha256"]
        ):
            raise ValueError("Combined corpus manifest changed")
        assets, records, summary = build_corpus(output, season)
        if (
            assets_path.read_bytes() != b"".join(encoded(a.model_dump(mode="json")) for a in assets)
            or docs_path.read_bytes() != b"".join(encoded(row) for row in records)
            or any(report.get(key) != value for key, value in summary.items())
        ):
            raise ValueError("Combined corpus no longer matches accepted inputs")
        return assets, records, summary
