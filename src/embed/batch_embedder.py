"""
Batch embedder with checkpointing and rate-limit management.

Processes a list of items (text chunks, image paths, audio paths, video paths)
in configurable batches, sleeping between batches to stay within Gemini's
rate limits. Progress is checkpointed so interrupted runs resume cleanly.

Output: data/embedded/{modality}/{stage_name}.jsonl
        (one JSON line per item: {"chunk_id": ..., "vector": [...], "metadata": {...}})
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

from config.settings import settings
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BatchEmbedder:
    """Embeds a list of items in batches with checkpointing.

    Attributes:
        stage_name: Identifier used for the checkpoint file and output JSONL.
        embed_fn: Callable that takes one item and returns a list of floats.
        output_path: Path to the JSONL output file.
        checkpoint: Tracks which item IDs have already been embedded.
        batch_size: Number of items per batch.
        delay: Seconds to sleep between batches.
    """

    def __init__(
        self,
        stage_name: str,
        embed_fn: Callable[[Any], list[float]],
        modality: str,
    ) -> None:
        """Initialise the batch embedder.

        Args:
            stage_name: Short name for this embedding run (e.g. ``text_chunks``).
            embed_fn: Function that accepts one item and returns its embedding.
            modality: Subdirectory under ``data/embedded/`` for output files.
        """
        self.stage_name = stage_name
        self.embed_fn = embed_fn
        self.batch_size = settings.embedding_batch_size
        self.delay = settings.embedding_delay_seconds

        output_dir = settings.embedded_dir / modality
        output_dir.mkdir(parents=True, exist_ok=True)
        self.output_path = output_dir / f"{stage_name}.jsonl"

        self.checkpoint = Checkpoint(f"embed_{stage_name}")

    def _append_result(self, chunk_id: str, vector: list[float], metadata: dict[str, Any]) -> None:
        """Append one embedded item to the JSONL output file.

        Args:
            chunk_id: Unique identifier for this item.
            vector: Embedding vector (list of floats).
            metadata: Associated metadata dict.
        """
        with open(self.output_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"chunk_id": chunk_id, "vector": vector, "metadata": metadata}) + "\n")

    def embed_all(self, items: list[dict[str, Any]], id_key: str = "chunk_id") -> int:
        """Embed all items, skipping already-checkpointed ones.

        Args:
            items: List of dicts. Each must have an ``id_key`` field plus any
                metadata. The embedding function receives the full dict.
            id_key: Key in each item dict that holds the unique identifier.

        Returns:
            Number of newly embedded items in this run.
        """
        total = len(items)
        pending = [it for it in items if not self.checkpoint.is_completed(str(it[id_key]))]
        logger.info(
            "embedding items",
            stage=self.stage_name,
            total=total,
            pending=len(pending),
            already_done=total - len(pending),
        )

        newly_embedded = 0
        for batch_start in range(0, len(pending), self.batch_size):
            batch = pending[batch_start : batch_start + self.batch_size]

            for item in batch:
                item_id = str(item[id_key])
                try:
                    vector = self.embed_fn(item)
                    metadata = {k: v for k, v in item.items() if k != "vector"}
                    self._append_result(item_id, vector, metadata)
                    self.checkpoint.mark_completed(item_id)
                    newly_embedded += 1
                except Exception as exc:
                    logger.error(
                        "embedding failed for item",
                        item_id=item_id,
                        stage=self.stage_name,
                        error=str(exc),
                    )

            done = total - len(pending) + batch_start + len(batch)
            pct = done / total * 100 if total else 0
            logger.info(
                f"embedded {done}/{total} {self.stage_name} ({pct:.1f}%)",
                stage=self.stage_name,
            )

            if batch_start + self.batch_size < len(pending):
                time.sleep(self.delay)

        logger.info(
            "batch embedding complete",
            stage=self.stage_name,
            newly_embedded=newly_embedded,
        )
        return newly_embedded
