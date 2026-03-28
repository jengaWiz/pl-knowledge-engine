"""
Tests for src/store/chroma_store.py
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.store.chroma_store import ChromaStore


def _make_store(tmp_path):
    with patch("src.store.chroma_store.chromadb") as mock_chroma, \
         patch("src.store.chroma_store.settings") as ms:
        ms.embedded_dir = tmp_path / "embedded"

        mock_client = MagicMock()
        mock_chroma.PersistentClient.return_value = mock_client
        mock_collection = MagicMock()
        mock_client.get_or_create_collection.return_value = mock_collection
        mock_client.get_collection.return_value = mock_collection

        store = ChromaStore(persist_dir=str(tmp_path / "chroma"))
        store._mock_collection = mock_collection
        return store, mock_collection


class TestSanitizeMetadata:
    def test_removes_none_values(self):
        meta = {"a": None, "b": "hello"}
        result = ChromaStore._sanitize_metadata(meta)
        assert result["a"] == -1
        assert result["b"] == "hello"

    def test_converts_list_to_comma_string(self):
        meta = {"teams": ["Liverpool", "Aston Villa"]}
        result = ChromaStore._sanitize_metadata(meta)
        assert result["teams"] == "Liverpool,Aston Villa"

    def test_passes_through_scalars(self):
        meta = {"n": 42, "f": 3.14, "b": True, "s": "ok"}
        result = ChromaStore._sanitize_metadata(meta)
        assert result == meta

    def test_converts_unknown_type_to_str(self):
        meta = {"obj": object()}
        result = ChromaStore._sanitize_metadata(meta)
        assert isinstance(result["obj"], str)


class TestFormatResults:
    def test_formats_correctly(self):
        raw = {
            "ids": [["id1", "id2"]],
            "documents": [["doc1", "doc2"]],
            "metadatas": [[{"a": 1}, {"b": 2}]],
            "distances": [[0.1, 0.2]],
        }
        results = ChromaStore._format_results(raw)
        assert len(results) == 2
        assert results[0]["chunk_id"] == "id1"
        assert results[0]["distance"] == 0.1
        assert results[1]["metadata"] == {"b": 2}

    def test_handles_empty_results(self):
        raw = {"ids": [[]], "documents": [[]], "metadatas": [[]], "distances": [[]]}
        results = ChromaStore._format_results(raw)
        assert results == []


class TestLoadFromJsonl:
    def test_missing_file_returns_zero(self, tmp_path):
        store, _ = _make_store(tmp_path)
        count = store.load_from_jsonl(tmp_path / "nonexistent.jsonl", "text")
        assert count == 0

    def test_loads_all_lines(self, tmp_path):
        jsonl_path = tmp_path / "test.jsonl"
        lines = [
            {"chunk_id": "c1", "vector": [0.1, 0.2], "metadata": {"text": "hello"}},
            {"chunk_id": "c2", "vector": [0.3, 0.4], "metadata": {"text": "world"}},
        ]
        with open(jsonl_path, "w") as f:
            for line in lines:
                f.write(json.dumps(line) + "\n")

        store, mock_col = _make_store(tmp_path)
        count = store.load_from_jsonl(jsonl_path, "text")
        assert count == 2
        assert mock_col.upsert.called

    def test_collection_counts_returns_dict(self, tmp_path):
        store, mock_col = _make_store(tmp_path)
        mock_col.count.return_value = 10
        counts = store.collection_counts()
        assert len(counts) == 5
        assert all(v == 10 for v in counts.values())
