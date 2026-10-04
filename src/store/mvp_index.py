"""Versioned, source-linked text retrieval with a checksum-verified local model."""

import hashlib
import json
from pathlib import Path

import chromadb
from chromadb.utils.embedding_functions import ONNXMiniLM_L6_V2

from src.clean.corpus_quality import load_verified_corpus
from src.ingest.commentary import load_commentary

MODEL = "all-MiniLM-L6-v2"
MODEL_SHA256 = "913d7300ceae3b2dbc2c50d1de4baacab4be7b9380491c27fab7418616a16ec3"
DIMENSIONS = 384
INDEX_SCHEMA = 2


def embedding_function():
    if ONNXMiniLM_L6_V2.MODEL_NAME != MODEL or ONNXMiniLM_L6_V2._MODEL_SHA256 != MODEL_SHA256:
        raise ValueError("Local embedding model contract changed; review before reindexing")
    return ONNXMiniLM_L6_V2()


def dataset_id(output: Path, season: str) -> str:
    report = json.loads((output / "reports" / "mvp" / season / "quality.json").read_text())
    return hashlib.sha256(
        json.dumps(
            {
                "artifacts": report["artifact_sha256"],
                "schema": INDEX_SCHEMA,
                "commentary": load_commentary(output, season)[1],
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()


def summaries(corpus: dict) -> list[dict]:
    docs = []
    for match in corpus["matches"]:
        docs.append(
            {
                "id": match["id"],
                "text": (
                    f"{match['season']} Premier League, {match['date']}: {match['home_team']} "
                    f"{match['home_score']}–{match['away_score']} {match['away_team']}. "
                    f"Shots: {match['home_shots']}–{match['away_shots']}; "
                    f"shots on target: {match['home_shots_on_target']}–"
                    f"{match['away_shots_on_target']}."
                ),
                "metadata": {
                    "season": match["season"],
                    "type": "match",
                    "home_team": match["home_team"],
                    "away_team": match["away_team"],
                    "url": match["source_url"],
                    "source_sha256": match["source_sha256"],
                    "source_row": match["source_row"],
                },
            }
        )
    teams = sorted({row[key] for row in corpus["matches"] for key in ("home_team", "away_team")})
    for team in teams:
        fixtures = [
            row for row in corpus["matches"] if team in (row["home_team"], row["away_team"])
        ]
        wins = draws = goals_for = goals_against = 0
        for row in fixtures:
            home = row["home_team"] == team
            scored = row["home_score" if home else "away_score"]
            conceded = row["away_score" if home else "home_score"]
            goals_for += scored
            goals_against += conceded
            wins += scored > conceded
            draws += scored == conceded
        docs.append(
            {
                "id": f"pl:{fixtures[0]['season']}:team:{team}",
                "text": (
                    f"{fixtures[0]['season']} Premier League {team}: {len(fixtures)} matches, "
                    f"{wins} wins, {draws} draws, {len(fixtures) - wins - draws} losses; "
                    f"{wins * 3 + draws} points from results, {goals_for} goals for, "
                    f"{goals_against} against. Administrative point deductions are excluded."
                ),
                "metadata": {
                    "season": fixtures[0]["season"],
                    "type": "team",
                    "team": team,
                    "url": fixtures[0]["source_url"],
                    "source_sha256": fixtures[0]["source_sha256"],
                    "source_rows": json.dumps([row["source_row"] for row in fixtures]),
                },
            }
        )
    for player in corpus["players"]:
        apps = [row for row in corpus["appearances"] if row["player_id"] == player["id"]]
        goals = sum(row["goals"] for row in apps if row["goals"] is not None)
        assists = sum(row["assists"] for row in apps if row["assists"] is not None)
        minutes = sum(row["minutes"] for row in apps)
        docs.append(
            {
                "id": player["id"],
                "text": (
                    f"{player['season']} {player['name']} "
                    f"({player['web_name']}, {player['position']}), "
                    f"represented {', '.join(player['teams_represented'])}. Available focus-club "
                    f"match-stat records: {goals} goals, {assists} assists, {minutes} minutes. "
                    "Archive assists differ from FPL awarded assists."
                ),
                "metadata": {
                    "season": player["season"],
                    "type": "player",
                    "team": player["teams_represented"][0],
                    "other_team": player["teams_represented"][-1],
                    "url": player["source"]["url"],
                    "source_sha256": player["source"]["sha256"],
                    "source_row": player["source"]["row"],
                },
            }
        )
    return docs


def build_index(output: Path, season: str, *, embed=None) -> dict:
    corpus = load_verified_corpus(output, season)
    version = dataset_id(output, season)
    embed = embed or embedding_function()
    docs = summaries(corpus)
    commentary, _ = load_commentary(output, season)
    docs.extend(
        {
            "id": row["id"],
            "text": (
                f"{season} external publisher metadata from {row['publisher']}, "
                f"{row['published_at']}: {row['title']}. {row['limitation']}"
            ),
            "metadata": {
                "season": season,
                "type": "publisher_metadata",
                "url": row["url"],
                "team": row["teams"][0],
                "other_team": row["teams"][-1],
                "publisher": row["publisher"],
                "published_at": row["published_at"],
                "source_sha256": row["source_sha256"],
            },
        }
        for row in commentary
    )
    client = chromadb.PersistentClient(path=str(output / "stores" / "chroma"))
    collection_name = f"mvp_{season.replace('-', '_')}_{version[:16]}"
    collection = client.get_or_create_collection(
        collection_name,
        embedding_function=None,
        metadata={
            "hnsw:space": "cosine",
            "model": MODEL,
            "model_sha256": MODEL_SHA256,
            "dataset_id": version,
        },
    )
    if collection.metadata.get("model_sha256") != MODEL_SHA256:
        raise ValueError("Stored collection embedding model differs from contract")
    for start in range(0, len(docs), 32):
        batch = docs[start : start + 32]
        vectors = embed([doc["text"] for doc in batch])
        if len(vectors) != len(batch) or any(len(vector) != DIMENSIONS for vector in vectors):
            raise ValueError("Local embedding dimensions or batch count differ from contract")
        collection.upsert(
            ids=[doc["id"] for doc in batch],
            embeddings=vectors,
            documents=[doc["text"] for doc in batch],
            metadatas=[doc["metadata"] for doc in batch],
        )
    if collection.count() != len(docs):
        raise ValueError("Text index count does not match verified evidence")
    return {
        "valid": True,
        "season": season,
        "dataset_id": version,
        "collection": collection_name,
        "documents": len(docs),
        "commentary_documents": len(commentary),
        "model": MODEL,
        "model_sha256": MODEL_SHA256,
        "dimensions": DIMENSIONS,
    }


def search(output: Path, season: str, query: str, *, team: str = "", limit: int = 5) -> list[dict]:
    if not query.strip() or not 1 <= limit <= 50:
        raise ValueError("Search requires a nonempty query and limit between 1 and 50")
    load_verified_corpus(output, season)
    report = json.loads((output / "reports" / "mvp" / season / "stores.json").read_text())
    index = report["index"]
    if not report.get("valid") or index["dataset_id"] != dataset_id(output, season):
        raise ValueError("Text index is stale; reload the verified stores")
    if (
        index.get("model_sha256") != MODEL_SHA256
        or index.get("model") != MODEL
        or index.get("dimensions") != DIMENSIONS
    ):
        raise ValueError("Index embedding model differs from the query contract")
    client = chromadb.PersistentClient(path=str(output / "stores" / "chroma"))
    collection = client.get_collection(index["collection"], embedding_function=None)
    kwargs = {
        "query_embeddings": embedding_function()([query]),
        "n_results": min(limit, index["documents"]),
    }
    if team:
        kwargs["where"] = {
            "$or": [{"team": team}, {"other_team": team}, {"home_team": team}, {"away_team": team}]
        }
    result = collection.query(**kwargs)
    return [
        {
            "id": ident,
            "text": result["documents"][0][offset],
            "metadata": result["metadatas"][0][offset],
            "distance": result["distances"][0][offset],
        }
        for offset, ident in enumerate(result["ids"][0])
    ]
