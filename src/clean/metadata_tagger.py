"""
Metadata tagger for all modalities.

Provides:
- ChunkMetadata: frozen dataclass representing Appendix A metadata schema
- to_chroma_metadata(): flat dict for ChromaDB (no None values)
- make_chunk_id(): consistent chunk ID string
- tag_text_chunk(): factory for text/transcript chunks
- tag_audio_segment(): factory for audio segment chunks
- tag_image(): factory for image chunks
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ChunkMetadata:
    """Appendix A metadata schema for all embedded chunks.

    Attributes:
        chunk_id: Unique identifier for this chunk.
        source_type: One of "match_stats", "player_stats",
            "podcast_transcript", "podcast_audio", "image", "video_highlight".
        source_name: Human-readable source name (e.g., episode title).
        source_id: Machine-readable source identifier (e.g., youtube_id).
        date: ISO date string ``YYYY-MM-DD``.
        modality: One of "text", "image", "audio", "video".
        teams: Comma-separated focus team names mentioned in the chunk.
        players: Comma-separated player names mentioned in the chunk.
        gameweek: Fantasy Premier League gameweek number, or None.
        chunk_index: Zero-based index of this chunk within the source.
        total_chunks: Total number of chunks for the source document.
    """

    chunk_id: str
    source_type: str
    source_name: str
    source_id: str
    date: str
    modality: str
    teams: str
    players: str
    gameweek: int | None
    chunk_index: int
    total_chunks: int


def make_chunk_id(source_type: str, source_id: str, index: int) -> str:
    """Generate a consistent chunk ID string.

    Args:
        source_type: Type tag (e.g., ``podcast_transcript``).
        source_id: Source identifier (e.g., YouTube ID).
        index: Zero-based chunk index.

    Returns:
        Formatted chunk ID string, e.g. ``podcast_transcript_abc123_007``.
    """
    return f"{source_type}_{source_id}_{index:03d}"


def to_chroma_metadata(meta: ChunkMetadata) -> dict[str, Any]:
    """Convert a ChunkMetadata to a flat dict suitable for ChromaDB.

    ChromaDB metadata values must be str, int, float, or bool — no None.
    ``gameweek=None`` is stored as ``-1``.

    Args:
        meta: Source metadata dataclass.

    Returns:
        Flat dict with all values as ChromaDB-compatible primitives.
    """
    return {
        "chunk_id": meta.chunk_id,
        "source_type": meta.source_type,
        "source_name": meta.source_name,
        "source_id": meta.source_id,
        "date": meta.date,
        "modality": meta.modality,
        "teams": meta.teams,
        "players": meta.players,
        "gameweek": meta.gameweek if meta.gameweek is not None else -1,
        "chunk_index": meta.chunk_index,
        "total_chunks": meta.total_chunks,
    }


def tag_text_chunk(
    text: str,
    source_type: str,
    source_name: str,
    source_id: str,
    date: str,
    chunk_index: int,
    total_chunks: int,
    teams: str = "",
    players: str = "",
    gameweek: int | None = None,
) -> ChunkMetadata:
    """Create metadata for a text/transcript chunk.

    Args:
        text: The chunk text (used only for ID generation; entity detection
            is performed upstream by the chunker).
        source_type: Source type tag.
        source_name: Human-readable source name.
        source_id: Machine-readable source identifier.
        date: ISO date string ``YYYY-MM-DD``.
        chunk_index: Zero-based index within the source.
        total_chunks: Total number of chunks.
        teams: Comma-separated team names (default empty).
        players: Comma-separated player names (default empty).
        gameweek: FPL gameweek number (default None).

    Returns:
        A populated ``ChunkMetadata`` instance.
    """
    return ChunkMetadata(
        chunk_id=make_chunk_id(source_type, source_id, chunk_index),
        source_type=source_type,
        source_name=source_name,
        source_id=source_id,
        date=date,
        modality="text",
        teams=teams,
        players=players,
        gameweek=gameweek,
        chunk_index=chunk_index,
        total_chunks=total_chunks,
    )


def tag_audio_segment(segment_meta: dict[str, Any], total: int) -> ChunkMetadata:
    """Create metadata for an audio segment chunk.

    Args:
        segment_meta: Segment dict produced by ``AudioSegmenter`` containing
            at minimum: ``segment_id``, ``source_id``, ``source_name``,
            ``date``, ``segment_index`` (or ``chunk_index``).
        total: Total number of segments for the source episode.

    Returns:
        A populated ``ChunkMetadata`` instance.
    """
    index = segment_meta.get("segment_index", segment_meta.get("chunk_index", 0))
    source_id = str(segment_meta.get("source_id", ""))
    return ChunkMetadata(
        chunk_id=segment_meta.get("segment_id", make_chunk_id("podcast_audio", source_id, index)),
        source_type="podcast_audio",
        source_name=str(segment_meta.get("source_name", "")),
        source_id=source_id,
        date=str(segment_meta.get("date", "")),
        modality="audio",
        teams=str(segment_meta.get("teams", "")),
        players=str(segment_meta.get("players", "")),
        gameweek=segment_meta.get("gameweek"),
        chunk_index=index,
        total_chunks=total,
    )


def tag_image(
    image_path: Path,
    source_type: str,
    source_id: str,
    date: str,
    teams: str = "",
    players: str = "",
) -> ChunkMetadata:
    """Create metadata for an image chunk.

    Args:
        image_path: Path to the image file (stem used as source name).
        source_type: Type tag, e.g. ``"image"`` or ``"team_badge"``.
        source_id: Machine-readable source identifier.
        date: ISO date string ``YYYY-MM-DD``.
        teams: Comma-separated team names.
        players: Comma-separated player names.

    Returns:
        A populated ``ChunkMetadata`` instance.
    """
    return ChunkMetadata(
        chunk_id=make_chunk_id(source_type, source_id, 0),
        source_type=source_type,
        source_name=image_path.stem,
        source_id=source_id,
        date=date,
        modality="image",
        teams=teams,
        players=players,
        gameweek=None,
        chunk_index=0,
        total_chunks=1,
    )
