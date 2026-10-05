"""Verify real local graph/index stores and numerical API acceptance."""

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import requests
from neo4j import GraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from scripts.accept_mvp import DEFAULT_REFERENCES, verify_references
from src.ingest.source_download import atomic_write
from src.store.mvp_index import search


def verify_stores(output: Path, season: str, url: str) -> dict:
    if urlsplit(url).hostname not in ("localhost", "127.0.0.1", "::1"):
        raise ValueError("Acceptance tests require a local loopback demo URL")
    report = {"valid": False, "season": season, "checks": []}
    path = output / "reports/mvp" / season / "store_acceptance.json"
    try:
        reference = verify_references(output, season)
        if not reference["valid"]:
            raise ValueError("Independent references failed")
        stores = json.loads((path.parent / "stores.json").read_text())
        quality = json.loads((path.parent / "quality.json").read_text())
        expected_dataset = ":".join(
            quality["artifact_sha256"][key] for key in sorted(quality["artifact_sha256"])
        )
        with GraphDatabase.driver(
            settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)
        ) as driver:
            with driver.session() as session:
                counts = {
                    row["label"]: row["n"]
                    for row in session.run(
                        "MATCH(n) WHERE n.mvp_managed=true AND n.season=$season "
                        "RETURN labels(n)[0] AS label,count(n) AS n",
                        season=season,
                    )
                }
                versions = session.run(
                    "MATCH(n) WHERE n.mvp_managed=true AND n.season=$season "
                    "RETURN collect(DISTINCT n.dataset_id) AS versions",
                    season=season,
                ).single()["versions"]
                if counts != stores["graph"]["counts"] or versions != [expected_dataset]:
                    raise ValueError("Actual graph differs from verified corpus metadata")
                report["graph_counts"] = counts
                report["checks"].append("Graph counts and versions match the verified corpus")
            for route in ("/api/matches", "/api/graph/overview", "/api/graph/player/Salah"):
                response = requests.get(url + route, timeout=30)
                response.raise_for_status()
                data = response.json()
                if route.endswith("matches"):
                    if len(data) != 380:
                        raise ValueError("Fixture API coverage differs from full league")
                    first = data[0]["id"]
                    match = requests.get(url + "/api/graph/match/" + first, timeout=30)
                    match.raise_for_status()
                    if not match.json()["nodes"]:
                        raise ValueError("Match graph is empty")
                else:
                    ids = {node["id"] for node in data["nodes"]}
                    if not ids or any(
                        edge["source"] not in ids or edge["target"] not in ids
                        for edge in data["edges"]
                    ):
                        raise ValueError("Graph API has missing nodes or dangling edges")
                report["checks"].append(route + " passed")
            run = "acceptance:" + uuid4().hex
            try:
                with driver.session() as session:
                    session.run(
                        "UNWIND [1,2] AS i CREATE (:Player {player_id:$run+toString(i), "
                        "full_name:'Ambiguous Reference Player',web_name:'Ambiguous', "
                        "mvp_managed:true,season:$season,acceptance_run:$run})",
                        run=run,
                        season=season,
                    ).consume()
                response = requests.get(url + "/api/graph/player/Ambiguous", timeout=30)
                if response.status_code != 409:
                    raise ValueError("Ambiguous player query was not rejected")
                report["checks"].append("Ambiguous player names return 409")
            finally:
                with driver.session() as session:
                    session.run("MATCH(p:Player {acceptance_run:$run}) DELETE p", run=run).consume()
        refs = json.loads(DEFAULT_REFERENCES.read_text())
        for case in refs["cases"]:
            response = requests.post(url + "/api/analysis", json=case["request"], timeout=30)
            response.raise_for_status()
            value = response.json()
            if not value["sources"]:
                raise ValueError("API calculation has no evidence")
            for key in case["path"]:
                value = value[key]
            if case.get("projection"):
                value = [{key: row[key] for key in case["projection"]} for row in value]
            if value != case["expected"]:
                raise ValueError("API reference differs: " + case["question"])
        hits = search(output, season, "Liverpool season points goals", team="Liverpool", limit=5)
        if not any(hit["metadata"]["type"] == "team" for hit in hits):
            raise ValueError("Source-linked local search did not retrieve the team summary")
        report["checks"].extend(
            ["42 numerical API references pass", "Local CPU semantic search passes"]
        )
        report.update(
            valid=True,
            reference_questions=len(refs["cases"]),
            artifact_sha256=quality["artifact_sha256"],
            index=stores["index"],
        )
    except Exception as exc:
        report["error"] = type(exc).__name__ + ": " + str(exc)
    atomic_write(path, (json.dumps(report, indent=2) + "\n").encode())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=settings.data_dir)
    parser.add_argument("--season", default=settings.season)
    parser.add_argument("--url", default="http://127.0.0.1:8011")
    args = parser.parse_args()
    report = verify_stores(args.output, args.season, args.url)
    print(f"Real-store acceptance: {report['valid']}; {len(report['checks'])} checks")
    if not report["valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
