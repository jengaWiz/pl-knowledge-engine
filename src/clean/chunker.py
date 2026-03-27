"""
Text chunker for embedding preparation.

Splits cleaned transcript text into overlapping word-level chunks sized for
Gemini Embedding 2. Attaches standardised Appendix A metadata to every chunk.

Output: data/cleaned/transcripts/{youtube_id}_chunks.json
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from config.settings import settings
from config.teams import FOCUS_TEAM_NAMES
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Known player names for entity detection
# ---------------------------------------------------------------------------
KNOWN_PLAYERS: list[str] = [
    "Salah", "Van Dijk", "Alisson", "Trent", "Alexander-Arnold",
    "Gakpo", "Díaz", "Diaz", "Núñez", "Nunez", "Mac Allister", "Gravenberch",
    "Slot", "Watkins", "McGinn", "Coutinho", "Bailey", "Duran",
    "Digne", "Cash", "Konsa", "Mings", "Pau Torres", "Emery",
    "Kamara", "Rogers", "Buendia", "Tielemans",
]


def _split_sentences(text: str) -> list[str]:
    """Split text into sentences on ``. ``, ``? ``, ``! `` boundaries.

    Args:
        text: Input text string.

    Returns:
        List of sentence strings (non-empty).
    """
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    return [s.strip() for s in sentences if s.strip()]


def _detect_teams(text: str) -> str:
    """Find focus team names mentioned in the text.

    Args:
        text: Chunk text to scan.

    Returns:
        Comma-separated string of team names found, or empty string.
    """
    text_lower = text.lower()
    found = [name for name in FOCUS_TEAM_NAMES if name.lower() in text_lower]
    return ",".join(found)


def _detect_players(text: str) -> str:
    """Find known player names mentioned in the text.

    Args:
        text: Chunk text to scan.

    Returns:
        Comma-separated string of detected player names.
    """
    found = [p for p in KNOWN_PLAYERS if p.lower() in text.lower()]
    # Deduplicate preserving order
    seen: set[str] = set()
    deduped = [p for p in found if not (p in seen or seen.add(p))]  # type: ignore[func-returns-value]
    return ",".join(deduped)


def chunk_text(
    text: str,
    chunk_size_words: int | None = None,
    overlap_words: int | None = None,
    min_chunk_words: int = 50,
) -> list[str]:
    """Split text into overlapping word-level chunks at sentence boundaries.

    Splits the text into sentences, then groups them into chunks of
    approximately ``chunk_size_words`` words with ``overlap_words`` words of
    overlap. Tail chunks shorter than ``min_chunk_words`` are merged into
    the previous chunk.

    Args:
        text: Input text to chunk.
        chunk_size_words: Target words per chunk (defaults to settings value).
        overlap_words: Words to overlap between consecutive chunks (defaults
            to settings value).
        min_chunk_words: Minimum word count for the last chunk before merging.

    Returns:
        List of chunk text strings.
    """
    chunk_size_words = chunk_size_words or settings.chunk_size_words
    overlap_words = overlap_words or settings.chunk_overlap_words

    sentences = _split_sentences(text)
    if not sentences:
        return []

    # Build word-indexed sentence boundaries
    words_per_sentence = [len(s.split()) for s in sentences]

    chunks: list[str] = []
    start_sent = 0

    while start_sent < len(sentences):
        word_count = 0
        end_sent = start_sent

        while end_sent < len(sentences) and word_count < chunk_size_words:
            word_count += words_per_sentence[end_sent]
            end_sent += 1

        chunk_sentences = sentences[start_sent:end_sent]
        chunks.append(" ".join(chunk_sentences))

        # Find the first sentence that starts after the overlap boundary
        overlap_count = 0
        step = end_sent - start_sent
        while step > 1 and overlap_count < overlap_words:
            step -= 1
            overlap_count += words_per_sentence[start_sent + step - 1] if (start_sent + step - 1) < len(words_per_sentence) else 0

        start_sent = end_sent - max(0, end_sent - start_sent - step)

        if start_sent >= end_sent:
            start_sent = end_sent  # Prevent infinite loop

    # Merge tail chunk that is too short
    if len(chunks) > 1 and len(chunks[-1].split()) < min_chunk_words:
        chunks[-2] = chunks[-2] + " " + chunks[-1]
        chunks.pop()

    return chunks


def build_chunk_metadata(
    chunks: list[str],
    youtube_id: str,
    title: str,
    channel: str,
    published_at: str,
) -> list[dict[str, Any]]:
    """Attach Appendix A metadata schema to each chunk.

    Args:
        chunks: List of chunk text strings.
        youtube_id: Source YouTube video ID.
        title: Full episode title.
        channel: Podcast channel name.
        published_at: ISO 8601 publication timestamp string.

    Returns:
        List of chunk dicts with ``chunk_id``, ``text``, and all metadata fields.
    """
    date = published_at[:10] if published_at else ""
    source_name = f"{channel} — {title}"
    total = len(chunks)

    result = []
    for i, text in enumerate(chunks):
        chunk_id = f"podcast_{youtube_id}_chunk_{i:03d}"
        result.append(
            {
                "chunk_id": chunk_id,
                "text": text,
                "source_type": "podcast_transcript",
                "source_name": source_name,
                "source_id": youtube_id,
                "date": date,
                "modality": "text",
                "teams": _detect_teams(text),
                "players": _detect_players(text),
                "gameweek": None,
                "chunk_index": i,
                "total_chunks": total,
                "word_count": len(text.split()),
            }
        )
    return result


class Chunker:
    """Splits cleaned transcript files into overlapping chunks with metadata.

    Attributes:
        cleaned_dir: Directory containing ``{youtube_id}_cleaned.txt`` files.
        output_dir: Directory to write ``{youtube_id}_chunks.json`` files.
    """

    def __init__(self) -> None:
        """Initialise paths from settings."""
        self.cleaned_dir = settings.cleaned_dir / "transcripts"
        self.output_dir = settings.cleaned_dir / "transcripts"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def chunk_episode(self, episode: dict[str, Any]) -> list[dict[str, Any]]:
        """Chunk a single episode's cleaned transcript.

        Args:
            episode: Episode metadata dict (must include ``youtube_id``,
                ``title``, ``channel``, ``published_at``).

        Returns:
            List of chunk metadata dicts. Empty list if no cleaned file exists.
        """
        vid_id = episode["youtube_id"]
        cleaned_path = self.cleaned_dir / f"{vid_id}_cleaned.txt"

        if not cleaned_path.exists():
            logger.warning("cleaned transcript not found", youtube_id=vid_id)
            return []

        text = cleaned_path.read_text(encoding="utf-8")
        chunks = chunk_text(text)
        metadata = build_chunk_metadata(
            chunks,
            youtube_id=vid_id,
            title=episode.get("title", ""),
            channel=episode.get("channel", ""),
            published_at=episode.get("published_at", ""),
        )

        out_path = self.output_dir / f"{vid_id}_chunks.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logger.info(
            "episode chunked",
            youtube_id=vid_id,
            chunks=len(metadata),
            avg_words=sum(c["word_count"] for c in metadata) // max(len(metadata), 1),
        )
        return metadata

    def chunk_all(self, episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Chunk all episodes and return the combined chunk list.

        Args:
            episodes: Episode manifest list.

        Returns:
            Flat list of all chunk metadata dicts across all episodes.
        """
        all_chunks: list[dict[str, Any]] = []
        for episode in episodes:
            all_chunks.extend(self.chunk_episode(episode))
        logger.info("chunking complete", total_chunks=len(all_chunks))
        return all_chunks


def run_chunking(episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Orchestrate chunking for all episodes.

    Args:
        episodes: Episode manifest list from the YouTube search stage.

    Returns:
        All chunk metadata dicts.
    """
    chunker = Chunker()
    return chunker.chunk_all(episodes)
