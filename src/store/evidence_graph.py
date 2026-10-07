"""Owned evidence projections and source links without rewriting the MVP graph."""

from collections import Counter, defaultdict
from pathlib import Path

from filelock import FileLock
from neo4j import GraphDatabase, Query, unit_of_work

from src.clean.corpus_quality import load_verified_corpus
from src.corpus.acceptance import load_corpus
from src.corpus.contracts import sha256
from src.corpus.statistical_text import encoded
from src.ingest.match_history import build_history, load_history_sources
from src.ingest.source_download import atomic_write

SCHEMA = "evidence-graph-v1"
LABELS = {"Corpus", "Source", "Document", "Season", "Entity"}
RELATIONS = {
    "HAS_SOURCE",
    "HAS_DOCUMENT",
    "HAS_ENTITY",
    "HAS_SEASON",
    "IN_SEASON",
    "DESCRIBES",
    "CITES",
    "HOME_TEAM",
    "AWAY_TEAM",
    "OF_PLAYER",
    "IN_MATCH",
    "FOR_TEAM",
}
BRIDGES = [("Match", "match_id"), ("Player", "player_id"), ("PlayerAppearance", "appearance_id")]


def graph_plan(assets, records, current, historical, primary_season):
    owner = "pl-evidence:" + primary_season
    nodes, edges = {}, {}

    def key(kind, ident):
        return owner + ":" + kind + ":" + ident

    def node(kind, ident, **props):
        ident = key(kind, ident)
        row = {"key": ident, "kind": kind, "props": {"kind": kind, **props}}
        if ident in nodes and nodes[ident] != row:
            raise ValueError("Evidence graph identity has conflicting properties")
        nodes[ident] = row
        return ident

    def edge(start, relation, end, **props):
        if relation not in RELATIONS:
            raise ValueError("Unsupported evidence relationship")
        ident = sha256(encoded([start, relation, end]))
        row = {
            "key": owner + ":edge:" + ident,
            "start": start,
            "end": end,
            "type": relation,
            "props": props,
        }
        if ident in edges and edges[ident] != row:
            raise ValueError("Evidence relationship has conflicting provenance")
        edges[ident] = row

    root = node("Corpus", primary_season, primary_season=primary_season, schema=SCHEMA)
    by_asset = {asset.id: asset for asset in assets}
    for asset in assets:
        if asset.parent_asset_id is None:
            source = node(
                "Source",
                asset.id,
                asset_id=asset.id,
                sha256=asset.sha256,
                source_url=asset.source_url,
                asset_json=encoded(asset.model_dump(mode="json")).decode(),
            )
            edge(root, "HAS_SOURCE", source)
    seasons = {}
    entities = {}

    def entity(ident, entity_type, season, **props):
        if season not in seasons:
            seasons[season] = node("Season", season, season=season)
            edge(root, "HAS_SEASON", seasons[season])
        result = node(
            "Entity", ident, canonical_id=ident, entity_type=entity_type, season=season, **props
        )
        entities[ident] = result
        edge(root, "HAS_ENTITY", result)
        edge(result, "IN_SEASON", seasons[season])
        return result

    matches = {match["id"]: match for match in [*current["matches"], *historical]}
    used_matches = {row["record_ids"][-1] for row in records}
    for ident in sorted(used_matches):
        match = matches[ident]
        home = entity(
            f"pl:{match['season']}:team:{match['home_team']}",
            "Team",
            match["season"],
            name=match["home_team"],
        )
        away = entity(
            f"pl:{match['season']}:team:{match['away_team']}",
            "Team",
            match["season"],
            name=match["away_team"],
        )
        fixture = entity(
            ident,
            "Match",
            match["season"],
            date=match["date"],
            home_team=match["home_team"],
            away_team=match["away_team"],
            home_score=match["home_score"],
            away_score=match["away_score"],
        )
        edge(fixture, "HOME_TEAM", home)
        edge(fixture, "AWAY_TEAM", away)
    players = {player["id"]: player for player in current["players"]}
    apps = {app["id"]: app for app in current["appearances"]}
    for record in records:
        if record["kind"] != "appearance":
            continue
        app = apps[record["record_ids"][0]]
        player = players[app["player_id"]]
        person = entity(
            player["id"],
            "Player",
            player["season"],
            name=player["name"],
            position=player["position"],
        )
        appearance = entity(
            app["id"],
            "PlayerAppearance",
            app["season"],
            date=app["date"],
            team=app["team"],
            minutes=app["minutes"],
            goals=app["goals"],
            assists=app["assists"],
            expected_goals=app["expected_goals"],
            expected_assists=app["expected_assists"],
        )
        edge(appearance, "OF_PLAYER", person)
        edge(appearance, "IN_MATCH", entities[app["match_id"]])
        edge(appearance, "FOR_TEAM", entities[f"pl:{app['season']}:team:{app['team']}"])
    for record in records:
        doc = record["document"]
        asset = by_asset[doc["asset_id"]]
        document = node(
            "Document",
            doc["id"],
            document_id=doc["id"],
            asset_id=asset.id,
            season=record["season"],
            date=record["event_date"],
            text=record["text"],
            sha256=doc["sha256"],
            document_json=encoded(doc).decode(),
            asset_json=encoded(asset.model_dump(mode="json")).decode(),
            source_refs_json=encoded(record["source_refs"]).decode(),
            record_kind=record["kind"],
        )
        edge(root, "HAS_DOCUMENT", document)
        edge(document, "IN_SEASON", seasons[record["season"]])
        for ident in record["record_ids"]:
            edge(document, "DESCRIBES", entities[ident])
        references = defaultdict(list)
        for ref in record["source_refs"]:
            if (
                ref["asset_id"] not in by_asset
                or by_asset[ref["asset_id"]].parent_asset_id is not None
            ):
                raise ValueError("Document citation is not an original source asset")
            references[ref["asset_id"]].append(ref)
        for asset_id, refs in references.items():
            edge(document, "CITES", key("Source", asset_id), refs_json=encoded(refs).decode())
    ordered_nodes = [nodes[ident] for ident in sorted(nodes)]
    ordered_edges = [edges[ident] for ident in sorted(edges)]
    if any(row["start"] not in nodes or row["end"] not in nodes for row in ordered_edges):
        raise ValueError("Evidence graph has a dangling internal relationship")
    dataset = sha256(encoded({"schema": SCHEMA, "nodes": ordered_nodes, "edges": ordered_edges}))
    return {
        "schema": SCHEMA,
        "owner": owner,
        "dataset_id": dataset,
        "nodes": ordered_nodes,
        "edges": ordered_edges,
        "counts": dict(Counter(row["kind"] for row in ordered_nodes)),
        "entity_counts": dict(
            Counter(row["props"]["entity_type"] for row in ordered_nodes if row["kind"] == "Entity")
        ),
        "relationships": len(ordered_edges),
    }


def prepare_graph(output, primary_season):
    assets, records, summary = load_corpus(output, primary_season)
    if not summary["target_met"]:
        raise ValueError("Evidence corpus target must pass before graph loading")
    current = load_verified_corpus(output, primary_season)
    _, _, seasons, _ = build_history(output, load_history_sources())
    plan = graph_plan(
        assets, records, current, [row for rows in seasons.values() for row in rows], primary_season
    )
    again_assets, again_records, _ = load_corpus(output, primary_season)
    if assets != again_assets or records != again_records:
        raise ValueError("Evidence changed during graph preparation")
    return plan


def properties(plan, row):
    return {
        key: value
        for key, value in {
            **row["props"],
            "evidence_key": row["key"],
            "evidence_owner": plan["owner"],
            "dataset_id": plan["dataset_id"],
        }.items()
        if value is not None
    }


def query(tx, text, **params):
    return tx.run(text, **params)


def verify_graph(tx, plan):
    nodes = query(
        tx,
        "MATCH (n:EvidenceNode {evidence_owner:$owner}) RETURN n.evidence_key AS key, "
        "properties(n) AS props,labels(n) AS labels",
        owner=plan["owner"],
    ).data()
    expected_labels = {
        row["key"]: {"EvidenceNode", "Evidence" + row["kind"]} for row in plan["nodes"]
    }
    if any(set(row["labels"]) != expected_labels.get(row["key"]) for row in nodes):
        raise ValueError("Evidence graph node labels changed")
    expected = {row["key"]: properties(plan, row) for row in plan["nodes"]}
    if {row["key"]: row["props"] for row in nodes} != expected:
        raise ValueError("Evidence graph nodes no longer match accepted source properties")
    edges = query(
        tx,
        """MATCH (a)-[r]->(b) WHERE r.evidence_owner=$owner AND type(r)<>'CANONICAL_RECORD'
RETURN a.evidence_key AS start,b.evidence_key AS end,type(r) AS type,properties(r) AS props""",
        owner=plan["owner"],
    ).data()
    expected_edges = {
        row["key"]: {
            "start": row["start"],
            "end": row["end"],
            "type": row["type"],
            "props": properties(plan, row),
        }
        for row in plan["edges"]
    }
    if (
        len(edges) != len(expected_edges)
        or {row["props"]["evidence_key"]: row for row in edges} != expected_edges
    ):
        raise ValueError("Evidence graph relationships no longer match accepted provenance")
    bridges = query(
        tx,
        """MATCH (a:EvidenceEntity)-[r:CANONICAL_RECORD]->(b)
WHERE r.evidence_owner=$owner
RETURN a.evidence_key AS source_key,a.evidence_owner AS source_owner,
a.canonical_id AS canonical_id,a.entity_type AS entity_type,labels(b) AS labels,
properties(b) AS target,r.dataset_id AS dataset""",
        owner=plan["owner"],
    ).data()
    fields = dict(BRIDGES)
    seen = set()
    for row in bridges:
        if row.get("source_key") in seen:
            raise ValueError("Canonical evidence link is duplicated")
        seen.add(row.get("source_key"))
        if (
            row.get("source_owner") != plan["owner"]
            or row.get("source_key") not in expected
            or expected.get(row.get("source_key"), {}).get("canonical_id") != row["canonical_id"]
            or expected.get(row.get("source_key"), {}).get("entity_type") != row["entity_type"]
            or row["target"].get("mvp_managed") is not True
            or row["entity_type"] not in fields
            or row["entity_type"] not in row["labels"]
            or row["target"].get(fields[row["entity_type"]]) != row["canonical_id"]
            or row["dataset"] != plan["dataset_id"]
        ):
            raise ValueError("Canonical evidence link differs from the existing entity")
    return len(bridges)


@unit_of_work(timeout=60)
def write_graph(tx, plan):
    for kind in sorted(LABELS):
        rows = [
            {"key": row["key"], "props": properties(plan, row)}
            for row in plan["nodes"]
            if row["kind"] == kind
        ]
        query(
            tx,
            f"UNWIND $rows AS row MERGE (n:EvidenceNode:Evidence{kind} "
            "{evidence_key:row.key}) SET n = row.props",
            rows=rows,
        ).consume()
    for relation in sorted(RELATIONS):
        rows = [
            {**row, "props": properties(plan, row)}
            for row in plan["edges"]
            if row["type"] == relation
        ]
        query(
            tx,
            f"""UNWIND $rows AS row MATCH (a:EvidenceNode {{evidence_key:row.start}}),
(b:EvidenceNode {{evidence_key:row.end}})
MERGE (a)-[r:{relation} {{evidence_key:row.key}}]->(b) SET r = row.props""",
            rows=rows,
        ).consume()
    for label, field in BRIDGES:
        query(
            tx,
            f"""MATCH (a:EvidenceEntity {{evidence_owner:$owner,entity_type:$label}}), (b:{label})
WHERE a.canonical_id=b.{field} AND b.mvp_managed=true
MERGE (a)-[r:CANONICAL_RECORD]->(b) SET r.evidence_owner=$owner,r.dataset_id=$dataset""",
            owner=plan["owner"],
            label=label,
            dataset=plan["dataset_id"],
        ).consume()
    query(
        tx,
        "MATCH (n:EvidenceNode {evidence_owner:$owner}) "
        "WHERE n.dataset_id<>$dataset DETACH DELETE n",
        owner=plan["owner"],
        dataset=plan["dataset_id"],
    ).consume()
    query(
        tx,
        "MATCH ()-[r]->() WHERE r.evidence_owner=$owner AND r.dataset_id<>$dataset DELETE r",
        owner=plan["owner"],
        dataset=plan["dataset_id"],
    ).consume()
    return verify_graph(tx, plan)


def report_path(output, season):
    return output / "reports/evidence" / season / "graph.json"


def load_graph(output: Path, season: str, uri: str, user: str, password: str):
    if not password:
        raise ValueError("Evidence graph requires NEO4J_PASSWORD")
    path = report_path(output, season)
    path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(path.parent / "graph.lock"), timeout=0):
        report = {"valid": False, "schema": SCHEMA, "primary_season": season}
        atomic_write(path, encoded(report))
        try:
            plan = prepare_graph(output, season)
            with GraphDatabase.driver(
                uri, auth=(user, password), connection_timeout=5, connection_acquisition_timeout=10
            ) as driver:
                driver.verify_connectivity()
                with driver.session() as session:
                    session.run(
                        Query(
                            "CREATE CONSTRAINT evidence_node_key IF NOT EXISTS "
                            "FOR (n:EvidenceNode) REQUIRE n.evidence_key IS UNIQUE",
                            timeout=30,
                        )
                    ).consume()
                    bridges = session.execute_write(write_graph, plan)
            report.update(
                valid=True,
                owner=plan["owner"],
                dataset_id=plan["dataset_id"],
                counts=plan["counts"],
                entity_counts=plan["entity_counts"],
                relationships=plan["relationships"],
                canonical_links=bridges,
            )
        except Exception as exc:
            report.update(error=type(exc).__name__, detail=str(exc)[:500])
            atomic_write(path, encoded(report))
            raise
        atomic_write(path, encoded(report))
        return report
