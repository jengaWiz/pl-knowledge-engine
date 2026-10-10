"""Small season-scoped graph reads for the populated MVP dashboard."""

from fastapi import HTTPException
from neo4j import GraphDatabase, Query
from neo4j.exceptions import DriverError, Neo4jError

from backend.player_search import resolve_player
from config.settings import settings

NODE_QUERY = "RETURN elementId(n) AS id, labels(n)[0] AS type, properties(n) AS props"


def node_view(row):
    props = row["props"]
    kind = row["type"]
    name = {
        "Season": props.get("label"),
        "Team": props.get("name"),
        "Player": props.get("web_name"),
        "Match": props.get("match_id"),
        "Gameweek": str(props.get("number")),
        "PlayerAppearance": props.get("name"),
    }[kind]
    result = {"id": row["id"], "name": name, "type": kind}
    fields = {
        "position": "position",
        "goals_scored": "goals",
        "assists": "assists",
        "home_team_name": "home",
        "away_team_name": "away",
        "gameweek": "gw",
        "number": "number",
        "home_score": "home_score",
        "away_score": "away_score",
        "date": "date",
        "minutes": "minutes",
        "team": "team",
        "total_points": "total_points",
    }
    if kind == "Player" and props.get("full_name"):
        result["full_name"] = props["full_name"]
    result.update({target: props[source] for source, target in fields.items() if source in props})
    return result


def connect():
    settings.require_credentials("neo4j_password")
    return GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
        connection_timeout=5,
        connection_acquisition_timeout=10,
    )


def query(session, cypher, **params):
    return session.run(Query(cypher, timeout=10), season=settings.season, **params).data()


def subgraph(session, rows):
    nodes = [node_view(row) for row in rows]
    ids = [row["id"] for row in rows]
    edges = query(
        session,
        """MATCH (a)-[r]->(b)
        WHERE a.season=$season AND b.season=$season AND r.mvp_managed=true
        AND elementId(a) IN $ids AND elementId(b) IN $ids
        RETURN elementId(a) AS source,elementId(b) AS target,type(r) AS type""",
        ids=ids,
    )
    return {"nodes": nodes, "edges": edges}


def read_graph(kind, key=""):
    try:
        with connect() as driver, driver.session() as session:
            if kind == "overview":
                rows = query(
                    session,
                    """MATCH (n) WHERE n.mvp_managed=true AND n.season=$season
                    AND NOT n:PlayerAppearance """
                    + NODE_QUERY,
                )
            elif kind == "player":
                roster = query(
                    session,
                    """MATCH (p:Player)
                    WHERE p.mvp_managed=true AND p.season=$season
                    RETURN p.player_id AS id,p.web_name AS name,p.full_name AS full_name""",
                )
                result = resolve_player(key.strip(), roster)
                if result["status"] != "matched":
                    raise HTTPException(
                        404 if result["status"] == "missing" else 409,
                        {
                            "message": "No stored player matches this name."
                            if result["status"] == "missing"
                            else "Choose a player from the closest roster matches.",
                            "suggestions": result["suggestions"],
                            "coverage": "Pinned 2025–26 Aston Villa and Liverpool roster only.",
                        },
                    )
                rows = query(
                    session,
                    """MATCH (p:Player {player_id:$id})
                    OPTIONAL MATCH (p)-[:PLAYS_FOR]->(t)-[:IN_SEASON]->(s)
                    OPTIONAL MATCH (p)-[:HAD_APPEARANCE]->(a)-[:IN_MATCH]->(m)-[:PART_OF]->(g)
                    UNWIND [p,t,s,a,m,g] AS n WITH DISTINCT n
                    WHERE n IS NOT NULL AND n.mvp_managed=true AND n.season=$season """
                    + NODE_QUERY,
                    id=result["player"]["id"],
                )
            else:
                rows = query(
                    session,
                    """MATCH (m:Match {match_id:$id})
                    WHERE m.mvp_managed=true AND m.season=$season
                    OPTIONAL MATCH (m)-[:PART_OF]->(g)
                    OPTIONAL MATCH (t)-[:HOME_TEAM|AWAY_TEAM]->(m)
                    OPTIONAL MATCH (p)-[:HAD_APPEARANCE]->(a)-[:IN_MATCH]->(m)
                    UNWIND [m,g,t,p,a] AS n WITH DISTINCT n
                    WHERE n IS NOT NULL AND n.mvp_managed=true AND n.season=$season """
                    + NODE_QUERY,
                    id=key,
                )
                if not rows:
                    raise HTTPException(404, "Match is unavailable in the verified season")
            return subgraph(session, rows)
    except (DriverError, Neo4jError, OSError, ValueError) as exc:
        raise HTTPException(
            503, "Local graph is unavailable; verify the database and reload stores"
        ) from exc


def fixtures():
    try:
        with connect() as driver, driver.session() as session:
            return query(
                session,
                """MATCH (m:Match)-[:PART_OF]->(g:Gameweek)
                WHERE m.mvp_managed=true AND m.season=$season
                RETURN m.match_id AS id,m.date AS date,m.home_team_name AS home_team,
                m.away_team_name AS away_team,m.home_score AS home_score,m.away_score AS away_score,
                g.number AS gameweek ORDER BY g.number,m.date,m.match_id""",
            )
    except (DriverError, Neo4jError, OSError, ValueError) as exc:
        raise HTTPException(
            503, "Local fixtures are unavailable; verify the database and reload stores"
        ) from exc
