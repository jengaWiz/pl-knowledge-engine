"""
Tests for src/embed/batch_embedder.py
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.embed.batch_embedder import BatchEmbedder


def _make_embedder(tmp_path, stage="test_stage"):
    with patch("src.embed.batch_embedder.settings") as ms, \
         patch("src.embed.batch_embedder.Checkpoint") as mock_cp:
        ms.embedded_dir = tmp_path / "embedded"
        ms.embedding_batch_size = 2
        ms.embedding_delay_seconds = 0
        ms.checkpoint_dir = tmp_path / "checkpoints"

        mock_cp.return_value.is_completed.return_value = False

        # embed_fn now accepts a list of items and returns a list of vectors
        embed_fn = MagicMock(return_value=[[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]])
        embedder = BatchEmbedder(stage, embed_fn, "text")
        embedder.checkpoint = mock_cp.return_value
        return embedder, embed_fn


class TestBatchEmbedder:
    def test_embeds_all_items(self, tmp_path):
        embedder, embed_fn = _make_embedder(tmp_path)
        items = [
            {"chunk_id": "c1", "text": "hello"},
            {"chunk_id": "c2", "text": "world"},
        ]
        count = embedder.embed_all(items)
        assert count == 2
        assert embed_fn.call_count == 1  # one batch call for both items

    def test_skips_checkpointed_items(self, tmp_path):
        embedder, embed_fn = _make_embedder(tmp_path)
        embedder.checkpoint.is_completed.side_effect = lambda x: x == "c1"
        embed_fn.return_value = [[0.1, 0.2, 0.3]]  # only one item in batch
        items = [
            {"chunk_id": "c1", "text": "already done"},
            {"chunk_id": "c2", "text": "new item"},
        ]
        count = embedder.embed_all(items)
        assert count == 1
        assert embed_fn.call_count == 1

    def test_writes_jsonl_output(self, tmp_path):
        embedder, _ = _make_embedder(tmp_path)
        embedder.embed_fn.return_value = [[0.1, 0.2, 0.3]]
        items = [{"chunk_id": "c1", "text": "test"}]
        embedder.embed_all(items)
        assert embedder.output_path.exists()
        lines = embedder.output_path.read_text().strip().split("\n")
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["chunk_id"] == "c1"
        assert "vector" in record

    def test_failed_embed_does_not_crash(self, tmp_path):
        embedder, embed_fn = _make_embedder(tmp_path)
        embed_fn.side_effect = RuntimeError("API error")
        items = [{"chunk_id": "c1", "text": "test"}]
        count = embedder.embed_all(items)
        assert count == 0  # Failed, but no exception raised

    def test_marks_checkpoint_after_success(self, tmp_path):
        embedder, _ = _make_embedder(tmp_path)
        embedder.embed_fn.return_value = [[0.1, 0.2, 0.3]]
        items = [{"chunk_id": "c1", "text": "test"}]
        embedder.embed_all(items)
        embedder.checkpoint.mark_completed.assert_called_once_with("c1")

    def test_returns_zero_for_empty_input(self, tmp_path):
        embedder, embed_fn = _make_embedder(tmp_path)
        count = embedder.embed_all([])
        assert count == 0
        embed_fn.assert_not_called()
