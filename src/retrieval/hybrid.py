"""Rank only graph-eligible evidence and join exact document/source identities."""

import json

from src.retrieval.routing import route
from src.store.evidence_graph import prepare_graph
from src.store.evidence_index import search
from src.store.evidence_queries import read

SCHEMA = "graph-constrained-retrieval-v1"


def retrieve(output, primary_season, query, uri, user, password, *, evidence_season, limit=5):
    if type(limit) is not int or not 1 <= limit <= 50:
        raise ValueError("Retrieval allows 1–50 results")
    plan = prepare_graph(output, primary_season)
    resolved = route(query, evidence_season, plan)
    response = {
        "schema": SCHEMA,
        "route": resolved,
        "hits": [],
        "graph_dataset_id": plan["dataset_id"],
    }
    if resolved["status"] != "resolved":
        return response
    # Gather a bounded canonical candidate set before embedding/ranking the query.
    graph_rows = read(
        output,
        primary_season,
        uri,
        user,
        password,
        evidence_season=evidence_season,
        kind=resolved["kind"],
        entity_id=resolved["entity_id"],
        opponent=resolved["opponent"],
        limit=50,
    )
    response["graph_candidates"] = len(graph_rows)
    response["candidate_limit"] = 50
    if not graph_rows:
        response["route"] = {
            **resolved,
            "status": "unavailable",
            "reason": "No graph-verified evidence matches this route.",
        }
    else:
        docs = {row["document_id"]: row for row in graph_rows}
        if len(docs) != len(graph_rows):
            raise ValueError("Graph candidate document identities are duplicated")
        fixture = resolved["kind"] == "fixture"
        vectors = search(
            output,
            primary_season,
            query,
            evidence_season=evidence_season,
            kind="match" if fixture else "appearance",
            player_id="" if fixture else resolved["entity_id"],
            match_ids=tuple(sorted({row["match_id"] for row in graph_rows})),
            limit=limit,
        )
        if len(vectors) != min(limit, len(docs)):
            raise ValueError("Vector and graph eligible document counts differ")
        seen = set()
        for hit in vectors:
            row = docs.get(hit["id"])
            metadata = hit["metadata"]
            if (
                row is None
                or hit["id"] in seen
                or hit["text"] != row["text"]
                or metadata["season"] != evidence_season
                or metadata["match_id"] != row["match_id"]
                or (not fixture and metadata["player_id"] != resolved["entity_id"])
                or json.loads(metadata["source_refs"]) != row["source_refs"]
            ):
                raise ValueError("Vector and graph document content, identity or provenance differ")
            seen.add(hit["id"])
            response["hits"].append({**hit, "graph": row})
    # No cross-store snapshot transaction exists: detect source changes across the reads.
    if prepare_graph(output, primary_season)["dataset_id"] != plan["dataset_id"]:
        raise ValueError("Accepted evidence changed during hybrid retrieval")
    return response
