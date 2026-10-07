"""A separate local text index over accepted, season-filtered football evidence."""

import json
from collections import Counter
from pathlib import Path

import chromadb
import numpy as np
from filelock import FileLock

from src.corpus.acceptance import load_corpus
from src.corpus.contracts import sha256
from src.corpus.statistical_text import encoded
from src.ingest.source_download import atomic_write
from src.store.mvp_index import DIMENSIONS, MODEL, MODEL_SHA256, embedding_function

SCHEMA = "evidence-onnx-index-v1"
BATCH = 32


def report_path(output: Path, season: str):
    return output / "reports/evidence" / season / "index.json"


def prepare_rows(output: Path, season: str):
    assets, records, summary = load_corpus(output, season)
    if not summary["target_met"]:
        raise ValueError("Structured corpus preparation target must pass before indexing")
    by_id = {asset.id: asset for asset in assets}
    rows = []
    for record in records:
        doc = record["document"]
        if doc["modality"] != "text" or sha256(record["text"].encode()) != doc["sha256"]:
            raise ValueError("Local evidence index only accepts checksum-verified text")
        parent = by_id[doc["asset_id"]]
        ids = record["record_ids"]
        appearance = record["kind"] == "appearance"
        metadata = {
            "asset_id": parent.id,
            "season": record["season"],
            "kind": record["kind"],
            "event_date": record["event_date"],
            "scope": parent.scope,
            "source_url": parent.source_url,
            "content_sha256": doc["sha256"],
            "source_refs": json.dumps(record["source_refs"], sort_keys=True),
            "record_ids": json.dumps(ids),
            "match_id": ids[-1],
            "player_id": ids[1] if appearance else "",
        }
        rows.append({"id": doc["id"], "text": record["text"], "metadata": metadata})
    rows.sort(key=lambda row: row["id"])
    profile = {
        "schema": SCHEMA,
        "model": MODEL,
        "model_sha256": MODEL_SHA256,
        "dimensions": DIMENSIONS,
        "distance": "cosine",
        "normalization": "l2-float32",
        "primary_season": season,
        "evidence_sha256": sha256(encoded(rows)),
        "target_policy": summary["target_policy"],
    }
    dataset = sha256(encoded(profile))
    return rows, profile, dataset, summary


def vectors_digest(vectors, count):
    array = np.asarray(vectors, dtype=np.float32)
    if array.shape != (count, DIMENSIONS) or not np.isfinite(array).all():
        raise ValueError("Embedding batch count, dimensions or finite values differ from contract")
    norms = np.linalg.norm(array, axis=1)
    if not np.isfinite(norms).all() or np.any(norms == 0):
        raise ValueError("Zero embedding vectors cannot define cosine retrieval")
    return sha256(array.tobytes()), array.tolist()


def normalized_vectors(vectors, count):
    _, checked = vectors_digest(vectors, count)
    array = np.asarray(checked, dtype=np.float32)
    return (array / np.linalg.norm(array, axis=1, keepdims=True)).tolist()


def stored_batch(collection, rows, expected=None):
    result = collection.get(
        ids=[row["id"] for row in rows], include=["documents", "metadatas", "embeddings"]
    )
    offsets = {ident: index for index, ident in enumerate(result["ids"])}
    if set(offsets) != {row["id"] for row in rows}:
        raise ValueError("Stored evidence batch is incomplete")
    vectors = []
    for row in rows:
        index = offsets[row["id"]]
        if (
            result["documents"][index] != row["text"]
            or result["metadatas"][index] != row["metadata"]
        ):
            raise ValueError("Stored evidence text or provenance changed")
        vectors.append(result["embeddings"][index])
    if expected is not None and not np.allclose(vectors, expected, rtol=1e-5, atol=1e-6):
        raise ValueError("Persisted vectors differ from generated embeddings")
    return vectors_digest(vectors, len(rows))[0]


def client(output):
    # Distinct directory: the established MVP Chroma collection is untouched.
    return chromadb.PersistentClient(path=str(output / "stores/evidence_chroma"))


def collection_metadata(profile, dataset):
    return {
        "hnsw:space": "cosine",
        "schema": SCHEMA,
        "model": MODEL,
        "model_sha256": MODEL_SHA256,
        "dimensions": DIMENSIONS,
        "dataset_id": dataset,
        "profile_sha256": sha256(encoded(profile)),
    }


def checkpoints(output, dataset, index):
    return output / "checkpoints/evidence_index" / dataset / f"batch_{index}.json"


def verify_index(output, collection, rows, profile, dataset):
    if collection.metadata != collection_metadata(profile, dataset):
        raise ValueError("Collection model or dataset differs from query contract")
    if collection.count() != len(rows):
        raise ValueError("Index record count differs from accepted evidence")
    digests = []
    for start in range(0, len(rows), BATCH):
        batch = rows[start : start + BATCH]
        checkpoint = json.loads(checkpoints(output, dataset, start).read_text())
        digest = stored_batch(collection, batch)
        if checkpoint != {"ids": [row["id"] for row in batch], "vectors_sha256": digest}:
            raise ValueError("Indexed vectors or checkpoint changed")
        digests.append(digest)
    return sha256(encoded(digests))


def build_index(output: Path, season: str, *, embed=None):
    path = report_path(output, season)
    path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(path.parent / "index.lock"), timeout=0):
        report = {"valid": False, "schema": SCHEMA, "primary_season": season}
        atomic_write(path, encoded(report))
        try:
            rows, profile, dataset, summary = prepare_rows(output, season)
            embed = embed or embedding_function()
            collection = client(output).get_or_create_collection(
                "evidence_onnx_" + dataset[:32],
                embedding_function=None,
                metadata=collection_metadata(profile, dataset),
            )
            if collection.metadata != collection_metadata(profile, dataset):
                raise ValueError("Collection embedding profile differs from contract")
            reused = 0
            for start in range(0, len(rows), BATCH):
                batch = rows[start : start + BATCH]
                checkpoint = checkpoints(output, dataset, start)
                if checkpoint.exists():
                    cached = json.loads(checkpoint.read_text())
                    if cached != {
                        "ids": [row["id"] for row in batch],
                        "vectors_sha256": stored_batch(collection, batch),
                    }:
                        raise ValueError("Cached index batch differs from accepted vectors")
                    reused += 1
                    continue
                vectors = normalized_vectors(embed([row["text"] for row in batch]), len(batch))
                collection.upsert(
                    ids=[row["id"] for row in batch],
                    embeddings=vectors,
                    documents=[row["text"] for row in batch],
                    metadatas=[row["metadata"] for row in batch],
                )
                digest = stored_batch(collection, batch, expected=vectors)
                atomic_write(
                    checkpoint,
                    encoded({"ids": [row["id"] for row in batch], "vectors_sha256": digest}),
                )
            digest = verify_index(output, collection, rows, profile, dataset)
            report.update(
                valid=True,
                profile=profile,
                dataset_id=dataset,
                collection=collection.name,
                indexed_records=len(rows),
                unique_documents=summary["counts"]["retrieval_documents"],
                records_by_season=dict(Counter(row["metadata"]["season"] for row in rows)),
                vectors_sha256=digest,
                reused_batches=reused,
            )
        except Exception as exc:
            report.update(error=type(exc).__name__, detail=str(exc)[:500])
            atomic_write(path, encoded(report))
            raise
        atomic_write(path, encoded(report))
        return report


def search(
    output: Path,
    primary_season: str,
    query: str,
    *,
    evidence_season: str,
    kind: str = "",
    player_id: str = "",
    match_id: str = "",
    match_ids: tuple[str, ...] = (),
    limit: int = 5,
):
    if not query.strip() or len(query) > 4000 or type(limit) is not int or not 1 <= limit <= 50:
        raise ValueError("Query must be nonempty, at most 4000 characters, with limit 1–50")
    if not evidence_season or kind not in {"", "match", "appearance"}:
        raise ValueError("Search requires an explicit evidence season and supported kind")
    if (
        not isinstance(match_ids, tuple)
        or len(match_ids) > 50
        or any(not isinstance(ident, str) or not ident for ident in match_ids)
        or (match_id and match_ids)
    ):
        raise ValueError("Use one match ID or a tuple of at most 50 nonempty match IDs")
    path = report_path(output, primary_season)
    with FileLock(str(path.parent / "index.lock"), timeout=0):
        rows, profile, dataset, _ = prepare_rows(output, primary_season)
        if evidence_season not in {row["metadata"]["season"] for row in rows}:
            raise ValueError("Requested season has no accepted evidence")
        report = json.loads(path.read_text())
        if (
            not report.get("valid")
            or report.get("profile") != profile
            or report.get("dataset_id") != dataset
            or report.get("indexed_records") != len(rows)
            or report.get("collection") != "evidence_onnx_" + dataset[:32]
        ):
            raise ValueError("Evidence index is stale or incomplete")
        collection = client(output).get_collection(report["collection"], embedding_function=None)
        if verify_index(output, collection, rows, profile, dataset) != report["vectors_sha256"]:
            raise ValueError("Evidence vector index changed after verification")
        vectors = normalized_vectors(embedding_function()([query]), 1)
        filters = [{"season": evidence_season}]
        filters.extend(
            {key: value}
            for key, value in (("kind", kind), ("player_id", player_id), ("match_id", match_id))
            if value
        )
        if match_ids:
            filters.append({"match_id": {"$in": sorted(set(match_ids))}})
        where = filters[0] if len(filters) == 1 else {"$and": filters}
        eligible = [
            row
            for row in rows
            if all(
                (
                    row["metadata"].get(key) in value["$in"]
                    if isinstance(value, dict)
                    else row["metadata"].get(key) == value
                )
                for clause in filters
                for key, value in clause.items()
            )
        ]
        if not eligible:
            return []
        result = collection.query(
            query_embeddings=vectors, n_results=min(limit, len(eligible)), where=where
        )
        return [
            {
                "id": ident,
                "text": result["documents"][0][index],
                "metadata": result["metadatas"][0][index],
                "distance": result["distances"][0][index],
            }
            for index, ident in enumerate(result["ids"][0])
        ]
