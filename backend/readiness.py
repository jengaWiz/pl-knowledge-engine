"""Readiness verifies current artifacts and both persistent stores."""

import json
from pathlib import Path

import chromadb
from chromadb.utils.embedding_functions import ONNXMiniLM_L6_V2

from backend.graph import connect, query
from config.settings import settings
from src.clean.corpus_quality import load_verified_corpus
from src.store.mvp_index import DIMENSIONS, MODEL, MODEL_SHA256, dataset_id


def readiness() -> dict:
    report = {
        "status": "not_ready",
        "season": settings.season,
        "checks": {"corpus": False, "graph": False, "index": False},
    }
    try:
        output, season = settings.data_dir, settings.season
        corpus = load_verified_corpus(output, season)
        base = output / "reports/mvp" / season
        quality = json.loads((base / "quality.json").read_text())
        stores = json.loads((base / "stores.json").read_text())
        report["checks"]["corpus"] = True
        if not stores.get("valid"):
            raise ValueError("Store loading has not passed")
        expected = ":".join(quality["artifact_sha256"][name] for name in sorted(corpus))
        with connect() as driver, driver.session() as session:
            rows = query(
                session,
                """MATCH(n) WHERE n.mvp_managed=true AND n.season=$season
                RETURN labels(n)[0] AS label,count(n) AS count,
                       collect(DISTINCT n.dataset_id) AS versions""",
            )
            counts = {row["label"]: row["count"] for row in rows}
            if counts != stores["graph"]["counts"] or any(
                row["versions"] != [expected] for row in rows
            ):
                raise ValueError("Graph is stale or incomplete")
        report["checks"]["graph"] = True
        index = stores["index"]
        if (
            index["dataset_id"] != dataset_id(output, season)
            or index["model"] != MODEL
            or index["model_sha256"] != MODEL_SHA256
            or index["dimensions"] != DIMENSIONS
        ):
            raise ValueError("Text index is stale or incompatible")
        collection = chromadb.PersistentClient(path=str(output / "stores/chroma")).get_collection(
            index["collection"], embedding_function=None
        )
        if (
            collection.count() != index["documents"]
            or collection.metadata.get("model_sha256") != MODEL_SHA256
        ):
            raise ValueError("Text index is incomplete")
        model_path = Path(ONNXMiniLM_L6_V2.DOWNLOAD_PATH) / ONNXMiniLM_L6_V2.EXTRACTED_FOLDER_NAME
        if not (model_path / "model.onnx").is_file():
            raise ValueError("Local model cache is unavailable")
        report["checks"]["index"] = True
        dates = sorted(row["date"] for row in corpus["matches"])
        commentary_path = base / "commentary.json"
        commentary = json.loads(commentary_path.read_text()) if commentary_path.exists() else {}
        report.update(
            status="ready",
            mode="local_evidence",
            counts=quality["counts"],
            date_range={"from": dates[0], "to": dates[-1]},
            index={key: index[key] for key in ("documents", "model", "dimensions", "dataset_id")},
            commentary={
                "status": commentary.get("external_commentary", "uncollected"),
                "coverage": commentary.get("coverage", {}),
            },
            quality_warnings=quality["warnings"],
        )
    except Exception:
        report["detail"] = (
            "Local data is not ready. Collect, validate and load stores; verify Neo4j is available."
        )
    return report
