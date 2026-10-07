"""Parameterized, season-scoped graph reads over integrity-verified evidence."""

import json

from filelock import FileLock
from neo4j import GraphDatabase, unit_of_work

from src.store.evidence_graph import SCHEMA, prepare_graph, query, report_path, verify_graph

FIXTURE = """MATCH (m:EvidenceEntity {canonical_id:$entity_id,entity_type:'Match',
    evidence_owner:$owner,season:$evidence_season})
MATCH (m)-[:HOME_TEAM]->(home:EvidenceEntity),(m)-[:AWAY_TEAM]->(away:EvidenceEntity)
MATCH (d:EvidenceDocument)-[:DESCRIBES]->(m)
WHERE d.record_kind='match' AND d.evidence_owner=$owner AND d.dataset_id=$dataset
RETURN d.document_id AS document_id,d.text AS text,d.source_refs_json AS source_refs_json,
    m.canonical_id AS match_id,m.date AS date,home.name AS home_team,away.name AS away_team,
    m.home_score AS home_score,m.away_score AS away_score LIMIT $limit"""

PLAYER = """MATCH (p:EvidenceEntity {canonical_id:$entity_id,entity_type:'Player',
    evidence_owner:$owner,season:$evidence_season})
MATCH (p)<-[:OF_PLAYER]-(a:EvidenceEntity)-[:IN_MATCH]->(m:EvidenceEntity)
MATCH (a)-[:FOR_TEAM]->(team:EvidenceEntity),(m)-[:HOME_TEAM|AWAY_TEAM]->(opponent:EvidenceEntity)
MATCH (d:EvidenceDocument)-[:DESCRIBES]->(a)
WHERE opponent<>team AND ($opponent='' OR opponent.name=$opponent)
    AND d.record_kind='appearance' AND d.evidence_owner=$owner AND d.dataset_id=$dataset
RETURN d.document_id AS document_id,d.text AS text,d.source_refs_json AS source_refs_json,
    p.canonical_id AS player_id,a.canonical_id AS appearance_id,m.canonical_id AS match_id,
    m.date AS date,team.name AS team,opponent.name AS opponent,a.minutes AS minutes
ORDER BY date DESC,match_id LIMIT $limit"""


def read(
    output,
    primary_season,
    uri,
    user,
    password,
    *,
    evidence_season,
    entity_id,
    kind="fixture",
    opponent="",
    limit=5,
    end_id="",
    max_hops=4,
):
    if not password:
        raise ValueError("Graph reads require NEO4J_PASSWORD")
    if kind not in {"fixture", "player", "path"} or not entity_id or not evidence_season:
        raise ValueError(
            "A supported graph read, canonical entity ID and explicit season are required"
        )
    if (
        type(limit) is not int
        or not 1 <= limit <= 50
        or type(max_hops) is not int
        or not 1 <= max_hops <= 4
    ):
        raise ValueError("Graph reads allow 1–50 results and 1–4 path hops")
    if kind == "path" and (not end_id or end_id == entity_id):
        raise ValueError("Paths require two distinct canonical entity IDs")
    path = report_path(output, primary_season)
    with FileLock(str(path.parent / "graph.lock"), timeout=0):
        plan = prepare_graph(output, primary_season)
        report = json.loads(path.read_text())
        if (
            not report.get("valid")
            or report.get("schema") != SCHEMA
            or report.get("dataset_id") != plan["dataset_id"]
            or report.get("counts") != plan["counts"]
            or report.get("relationships") != plan["relationships"]
        ):
            raise ValueError("Evidence graph is stale or incomplete")
        entities = {
            row["props"]["canonical_id"]: row["props"]
            for row in plan["nodes"]
            if row["kind"] == "Entity" and row["props"]["season"] == evidence_season
        }
        expected_type = {"fixture": "Match", "player": "Player"}.get(kind)
        if (
            entity_id not in entities
            or (expected_type and entities[entity_id]["entity_type"] != expected_type)
            or (kind == "path" and end_id not in entities)
        ):
            raise ValueError("Canonical entity is unavailable in the requested season")
        params = {
            "owner": plan["owner"],
            "dataset": plan["dataset_id"],
            "entity_id": entity_id,
            "evidence_season": evidence_season,
            "opponent": opponent,
            "limit": limit,
            "end_id": end_id,
        }

        @unit_of_work(timeout=10)
        def execute(tx):
            if verify_graph(tx, plan) != report["canonical_links"]:
                raise ValueError("Canonical graph references changed")
            if kind == "path":
                # Only the validated integer bound enters this fixed Cypher template.
                cypher = f"""MATCH (a:EvidenceEntity {{canonical_id:$entity_id,
                    evidence_owner:$owner,
                    season:$evidence_season}}),(b:EvidenceEntity {{canonical_id:$end_id,
                    evidence_owner:$owner,season:$evidence_season}})
MATCH p=shortestPath((a)-[:OF_PLAYER|IN_MATCH|FOR_TEAM|HOME_TEAM|AWAY_TEAM*1..{max_hops}]-(b))
WHERE all(n IN nodes(p) WHERE n.evidence_owner=$owner AND n.season=$evidence_season)
    AND all(r IN relationships(p) WHERE r.evidence_owner=$owner AND r.dataset_id=$dataset)
RETURN [n IN nodes(p) | {{canonical_id:n.canonical_id,entity_type:n.entity_type}}] AS nodes,
    [r IN relationships(p) | type(r)] AS relationships,length(p) AS hops LIMIT 1"""
            else:
                cypher = FIXTURE if kind == "fixture" else PLAYER
            rows = query(tx, cypher, **params).data()
            for row in rows:
                if "source_refs_json" in row:
                    row["source_refs"] = json.loads(row.pop("source_refs_json"))
            return rows

        with (
            GraphDatabase.driver(
                uri, auth=(user, password), connection_timeout=5, connection_acquisition_timeout=10
            ) as driver,
            driver.session() as session,
        ):
            return session.execute_read(execute)
