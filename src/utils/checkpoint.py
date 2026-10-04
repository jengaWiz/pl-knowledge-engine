"""
Checkpoint system for resumable pipeline stages.

Usage:
    cp = Checkpoint("embed_text_chunks")
    if cp.is_completed("chunk_042"):
        continue  # Skip already-processed items
    # ... process chunk_042 ...
    cp.mark_completed("chunk_042")
"""

import json

from config.settings import settings
from src.ingest.source_download import atomic_write
from src.utils.logger import get_logger

logger = get_logger(__name__)


class Checkpoint:
    """Tracks progress of a long-running pipeline stage to enable resumability.

    State is persisted to a JSON file in the checkpoint directory so that
    an interrupted run can continue from where it left off.

    Attributes:
        stage_name: Identifier for this pipeline stage (used as filename).
        filepath: Absolute path to the checkpoint JSON file.
        completed: Set of item IDs that have been successfully processed.
    """

    def __init__(self, stage_name: str) -> None:
        """Initialize a checkpoint for the given pipeline stage.

        Args:
            stage_name: Unique name for this stage, used as the checkpoint filename.
        """
        self.stage_name = stage_name
        self.filepath = settings.checkpoint_dir / f"{stage_name}.json"
        self.completed: set[str] = set()
        self._load()

    def _load(self) -> None:
        """Load existing checkpoint from disk."""
        if self.filepath.exists():
            with open(self.filepath) as f:
                data = json.load(f)
                completed = data.get("completed", [])
                if not isinstance(completed, list) or any(
                    not isinstance(item, str) for item in completed
                ):
                    raise ValueError("Invalid checkpoint format")
                self.completed = set(completed)
            logger.info(
                "checkpoint loaded",
                stage=self.stage_name,
                completed_count=len(self.completed),
            )

    def _save(self) -> None:
        """Persist checkpoint to disk."""
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(
            self.filepath, json.dumps({"completed": sorted(self.completed)}, indent=2).encode()
        )

    def is_completed(self, item_id: str) -> bool:
        """Check if an item has already been processed.

        Args:
            item_id: Unique identifier for the item to check.

        Returns:
            True if the item has been marked as completed, False otherwise.
        """
        return item_id in self.completed

    def mark_completed(self, item_id: str) -> None:
        """Mark an item as processed and save to disk.

        Args:
            item_id: Unique identifier for the item to mark as completed.
        """
        previous = self.completed.copy()
        self.completed.add(item_id)
        try:
            self._save()
        except BaseException:
            self.completed = previous
            raise

    def reset(self) -> None:
        """Clear all checkpoint data and remove the checkpoint file."""
        self.completed.clear()
        if self.filepath.exists():
            self.filepath.unlink()
        logger.info("checkpoint reset", stage=self.stage_name)
