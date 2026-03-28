"""
ChromaDB vector store.

Creates and manages five cosine-similarity collections:
    text_chunks      — match/player summaries + podcast transcript chunks
    images           — player headshots and team badges
    audio_segments   — podcast audio segment embeddings
    video_highlights — match highlight clip embeddings
    unified          — all vectors from all modalities (enables cross-modal search)

All insertions are idempotent by chunk_id (ChromaDB upserts on duplicate IDs).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import chromadb
from chromadb import Collection

from config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_COSINE = {"hnsw:space": "cosine"}

COLLECTION_TEXT = "text_chunks"
COLLECTION_IMAGES = "images"
COLLECTION_AUDIO = "audio_segments"
COLLECTION_VIDEO = "video_highlights"
COLLECTION_UNIFIED = "unified"


class ChromaStore:
    """Manages ChromaDB collections for all embedding modalities.

    Attributes:
        client: PersistentClient that saves data to ``chroma_data/`` on disk.
        text_chunks: Collection for text embeddings.
        images: Collection for image embeddings.
        audio_segments: Collection for audio embeddings.
        video_highlights: Collection for video embeddings.
        unified: Collection containing all vectors across modalities.
    """

    def __init__(self, persist_dir: str = "./chroma_data") -> None:
        """Initialise the ChromaDB client and create collections.

        Args:
            persist_dir: Directory where ChromaDB persists data to disk.
        """
        self.client = chromadb.PersistentClient(path=persist_dir)
        self.text_chunks: Collection = self.client.get_or_create_collection(
            COLLECTION_TEXT, metadata=_COSINE
        )
        self.images: Collection = self.client.get_or_create_collection(
            COLLECTION_IMAGES, metadata=_COSINE
        )
        self.audio_segments: Collection = self.client.get_or_create_collection(
            COLLECTION_AUDIO, metadata=_COSINE
        )
        self.video_highlights: Collection = self.client.get_or_create_collection(
            COLLECTION_VIDEO, metadata=_COSINE
        )
        self.unified: Collection = self.client.get_or_create_collection(
            COLLECTION_UNIFIED, metadata=_COSINE
        )
        logger.info(
            "chroma store initialised",
            persist_dir=persist_dir,
            collections=[COLLECTION_TEXT, COLLECTION_IMAGES, COLLECTION_AUDIO,
                         COLLECTION_VIDEO, COLLECTION_UNIFIED],
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_metadata(meta: dict[str, Any]) -> dict[str, Any]:
        """Strip non-scalar values so ChromaDB accepts the metadata dict.

        ChromaDB only allows str, int, float, or bool metadata values.
        Lists and None are not permitted.

        Args:
            meta: Raw metadata dict.

        Returns:
            Sanitized dict with only ChromaDB-compatible values.
        """
        clean: dict[str, Any] = {}
        for k, v in meta.items():
            if isinstance(v, (str, int, float, bool)):
                clean[k] = v
            elif v is None:
                clean[k] = -1  # None → -1 for numeric fields, "" for strings
            elif isinstance(v, list):
                clean[k] = ",".join(str(x) for x in v)
            else:
                clean[k] = str(v)
        return clean

    def _upsert(
        self,
        collection: Collection,
        ids: list[str],
        embeddings: list[list[float]],
        metadatas: list[dict[str, Any]],
        documents: list[str] | None = None,
    ) -> None:
        """Upsert a batch into a collection and the unified collection.

        Args:
            collection: Target modality collection.
            ids: List of unique chunk IDs.
            embeddings: Corresponding embedding vectors.
            metadatas: Corresponding metadata dicts.
            documents: Optional text strings (only for text collections).
        """
        clean_meta = [self._sanitize_metadata(m) for m in metadatas]
        kwargs: dict[str, Any] = dict(
            ids=ids, embeddings=embeddings, metadatas=clean_meta
        )
        if documents:
            kwargs["documents"] = documents

        collection.upsert(**kwargs)

        # Also insert into unified (no documents for non-text modalities)
        self.unified.upsert(ids=ids, embeddings=embeddings, metadatas=clean_meta)

    # ------------------------------------------------------------------
    # Load from JSONL files
    # ------------------------------------------------------------------

    def load_from_jsonl(self, jsonl_path: Path, modality: str) -> int:
        """Load embedded vectors from a JSONL file into the correct collection.

        Each line in the JSONL must be:
            {"chunk_id": "...", "vector": [...], "metadata": {...}}

        Args:
            jsonl_path: Path to the ``.jsonl`` file produced by BatchEmbedder.
            modality: One of ``"text"``, ``"images"``, ``"audio"``, ``"video"``.

        Returns:
            Number of items loaded.
        """
        if not jsonl_path.exists():
            logger.warning("jsonl file not found", path=str(jsonl_path))
            return 0

        collection_map = {
            "text": self.text_chunks,
            "images": self.images,
            "audio": self.audio_segments,
            "video": self.video_highlights,
        }
        collection = collection_map.get(modality, self.text_chunks)
        is_text = modality == "text"

        ids, embeddings, metadatas, documents = [], [], [], []
        loaded = 0

        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                item = json.loads(line)
                ids.append(item["chunk_id"])
                embeddings.append(item["vector"])
                metadatas.append(item.get("metadata", {}))
                if is_text:
                    documents.append(item["metadata"].get("text", ""))
                loaded += 1

                # Upsert in batches of 100 to avoid memory issues
                if len(ids) >= 100:
                    self._upsert(
                        collection, ids, embeddings, metadatas,
                        documents if is_text else None,
                    )
                    ids, embeddings, metadatas, documents = [], [], [], []

        if ids:
            self._upsert(
                collection, ids, embeddings, metadatas,
                documents if is_text else None,
            )

        logger.info("loaded vectors", path=str(jsonl_path), modality=modality, count=loaded)
        return loaded

    def load_all_embedded(self) -> dict[str, int]:
        """Scan ``data/embedded/`` and load all JSONL files into ChromaDB.

        Returns:
            Dict mapping JSONL filename stem to the number of items loaded.
        """
        embedded_dir = settings.embedded_dir
        modality_dirs = {
            "text": embedded_dir / "text",
            "images": embedded_dir / "images",
            "audio": embedded_dir / "audio",
            "video": embedded_dir / "video",
        }

        results: dict[str, int] = {}
        for modality, directory in modality_dirs.items():
            if not directory.exists():
                continue
            for jsonl_path in sorted(directory.glob("*.jsonl")):
                count = self.load_from_jsonl(jsonl_path, modality)
                results[jsonl_path.stem] = count

        logger.info("all embedded data loaded", results=results)
        return results

    # ------------------------------------------------------------------
    # Search methods
    # ------------------------------------------------------------------

    def search_text(
        self,
        query_vector: list[float],
        n_results: int = 10,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Search the text_chunks collection by vector.

        Args:
            query_vector: Embedding of the query.
            n_results: Number of results to return.
            filters: Optional ChromaDB ``where`` filter dict.

        Returns:
            List of result dicts with ``chunk_id``, ``document``, ``metadata``,
            and ``distance`` keys.
        """
        kwargs: dict[str, Any] = dict(
            query_embeddings=[query_vector], n_results=n_results
        )
        if filters:
            kwargs["where"] = filters
        return self._format_results(self.text_chunks.query(**kwargs))

    def search_by_vector(
        self,
        query_vector: list[float],
        collection_name: str,
        n_results: int = 10,
    ) -> list[dict[str, Any]]:
        """Search any named collection by vector.

        Args:
            query_vector: Embedding of the query.
            collection_name: One of the five collection name constants.
            n_results: Number of results to return.

        Returns:
            List of result dicts.
        """
        collection = self.client.get_collection(collection_name)
        results = collection.query(
            query_embeddings=[query_vector], n_results=n_results
        )
        return self._format_results(results)

    def search_unified(
        self,
        query_vector: list[float],
        n_results: int = 10,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Search across all modalities in the unified collection.

        Args:
            query_vector: Embedding of the query.
            n_results: Number of results to return.
            filters: Optional ChromaDB ``where`` filter dict.

        Returns:
            List of result dicts sorted by similarity.
        """
        kwargs: dict[str, Any] = dict(
            query_embeddings=[query_vector], n_results=n_results
        )
        if filters:
            kwargs["where"] = filters
        return self._format_results(self.unified.query(**kwargs))

    @staticmethod
    def _format_results(raw: dict[str, Any]) -> list[dict[str, Any]]:
        """Flatten ChromaDB query results into a list of dicts.

        Args:
            raw: Raw ChromaDB query response.

        Returns:
            List of result dicts with ``chunk_id``, ``document``,
            ``metadata``, and ``distance``.
        """
        ids = (raw.get("ids") or [[]])[0]
        docs = (raw.get("documents") or [[None] * len(ids)])[0]
        metas = (raw.get("metadatas") or [[{}] * len(ids)])[0]
        distances = (raw.get("distances") or [[None] * len(ids)])[0]

        return [
            {
                "chunk_id": ids[i],
                "document": docs[i],
                "metadata": metas[i],
                "distance": distances[i],
            }
            for i in range(len(ids))
        ]

    def collection_counts(self) -> dict[str, int]:
        """Return the number of vectors in each collection.

        Returns:
            Dict mapping collection name to item count.
        """
        return {
            COLLECTION_TEXT: self.text_chunks.count(),
            COLLECTION_IMAGES: self.images.count(),
            COLLECTION_AUDIO: self.audio_segments.count(),
            COLLECTION_VIDEO: self.video_highlights.count(),
            COLLECTION_UNIFIED: self.unified.count(),
        }
