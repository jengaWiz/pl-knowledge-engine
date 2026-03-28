"""
Embedding orchestrator.

Runs the full embedding pipeline across all modalities in sequence:
    1. Text  — match summaries, player summaries, podcast transcript chunks
    2. Images — player headshots and team badges
    3. Audio  — podcast audio segments
    4. Video  — match highlight clips

Each modality's vectors are written to data/embedded/{modality}/{stage}.jsonl
and checkpointed so interrupted runs resume cleanly.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config.settings import settings
from src.embed.gemini_client import GeminiEmbedder
from src.embed.batch_embedder import BatchEmbedder
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _load_json(path: Path) -> list[dict[str, Any]]:
    """Load a JSON file as a list. Returns [] if file does not exist."""
    if not path.exists():
        logger.warning("file not found, skipping", path=str(path))
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    """Load a JSONL file as a list. Returns [] if file does not exist."""
    if not path.exists():
        return []
    items = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                items.append(json.loads(line))
    return items


def _collect_transcript_chunks() -> list[dict[str, Any]]:
    """Gather all podcast transcript chunk JSON files into one flat list."""
    transcript_dir = settings.cleaned_dir / "transcripts"
    chunks: list[dict[str, Any]] = []
    for path in sorted(transcript_dir.glob("*_chunks.json")):
        chunks.extend(_load_json(path))
    logger.info("collected transcript chunks", count=len(chunks))
    return chunks


def _collect_image_items() -> list[dict[str, Any]]:
    """Build embedding items for all images listed in images_metadata.json."""
    meta_path = settings.raw_dir / "images" / "images_metadata.json"
    metadata = _load_json(meta_path)
    items = []
    for entry in metadata:
        file_path = entry.get("file_path") or entry.get("path", "")
        if file_path and Path(file_path).exists():
            items.append({
                "chunk_id": entry.get("chunk_id") or f"image_{Path(file_path).stem}",
                "file_path": file_path,
                "source_type": entry.get("source_type", "image"),
                "teams": entry.get("teams", ""),
                "players": entry.get("players", ""),
                "date": entry.get("date", ""),
                "modality": "image",
            })
    logger.info("collected image items", count=len(items))
    return items


def _collect_audio_items() -> list[dict[str, Any]]:
    """Build embedding items from all audio segment metadata files."""
    audio_dir = settings.cleaned_dir / "audio_segments"
    items: list[dict[str, Any]] = []
    for meta_path in sorted(audio_dir.glob("*_segments_meta.json")):
        segments = _load_json(meta_path)
        for seg in segments:
            if Path(seg.get("file_path", "")).exists():
                items.append(seg)
    logger.info("collected audio items", count=len(items))
    return items


def _collect_video_items() -> list[dict[str, Any]]:
    """Build embedding items from video_metadata.json."""
    meta_path = settings.raw_dir / "video" / "video_metadata.json"
    metadata = _load_json(meta_path)
    items = []
    for entry in metadata:
        file_path = entry.get("file_path") or entry.get("path", "")
        if file_path and Path(file_path).exists():
            items.append({
                "chunk_id": entry.get("chunk_id") or f"video_{Path(file_path).stem}",
                "file_path": file_path,
                "source_type": "video_highlight",
                "teams": entry.get("teams", ""),
                "date": entry.get("date", ""),
                "modality": "video",
            })
    logger.info("collected video items", count=len(items))
    return items


def run_embedding_pipeline() -> dict[str, int]:
    """Run the full embedding pipeline across all modalities.

    Returns:
        Dict mapping modality name to the number of newly embedded items.
    """
    embedder = GeminiEmbedder()
    results: dict[str, int] = {}

    # ------------------------------------------------------------------
    # 1. Text — match summaries
    # ------------------------------------------------------------------
    match_summaries = _load_json(settings.cleaned_dir / "stats" / "match_summaries.json")
    if match_summaries:
        def embed_text_item(item: dict) -> list[float]:
            return embedder.embed_text(item["text"])

        be = BatchEmbedder("match_summaries", embed_text_item, "text")
        results["match_summaries"] = be.embed_all(match_summaries)

    # ------------------------------------------------------------------
    # 2. Text — player summaries
    # ------------------------------------------------------------------
    player_summaries = _load_json(settings.cleaned_dir / "stats" / "player_summaries.json")
    if player_summaries:
        def embed_text_item(item: dict) -> list[float]:  # noqa: F811
            return embedder.embed_text(item["text"])

        be = BatchEmbedder("player_summaries", embed_text_item, "text")
        results["player_summaries"] = be.embed_all(player_summaries)

    # ------------------------------------------------------------------
    # 3. Text — podcast transcript chunks
    # ------------------------------------------------------------------
    transcript_chunks = _collect_transcript_chunks()
    if transcript_chunks:
        def embed_text_item(item: dict) -> list[float]:  # noqa: F811
            return embedder.embed_text(item["text"])

        be = BatchEmbedder("transcript_chunks", embed_text_item, "text")
        results["transcript_chunks"] = be.embed_all(transcript_chunks)

    # ------------------------------------------------------------------
    # 4. Images
    # ------------------------------------------------------------------
    image_items = _collect_image_items()
    if image_items:
        def embed_image_item(item: dict) -> list[float]:
            return embedder.embed_image(Path(item["file_path"]))

        be = BatchEmbedder("images", embed_image_item, "images")
        results["images"] = be.embed_all(image_items)

    # ------------------------------------------------------------------
    # 5. Audio segments
    # ------------------------------------------------------------------
    audio_items = _collect_audio_items()
    if audio_items:
        def embed_audio_item(item: dict) -> list[float]:
            return embedder.embed_audio(Path(item["file_path"]))

        be = BatchEmbedder("audio_segments", embed_audio_item, "audio")
        results["audio_segments"] = be.embed_all(audio_items)

    # ------------------------------------------------------------------
    # 6. Video highlights
    # ------------------------------------------------------------------
    video_items = _collect_video_items()
    if video_items:
        def embed_video_item(item: dict) -> list[float]:
            return embedder.embed_video(Path(item["file_path"]))

        be = BatchEmbedder("video_highlights", embed_video_item, "video")
        results["video_highlights"] = be.embed_all(video_items)

    logger.info("embedding pipeline complete", results=results)
    return results
