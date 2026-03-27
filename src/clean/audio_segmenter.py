"""
Audio segmenter.

Splits full podcast MP3 files into overlapping segments suitable for
Gemini Embedding 2. Uses pydub for audio manipulation (requires ffmpeg).

Output:
    data/cleaned/audio_segments/{youtube_id}_seg_{NNN}.mp3
    data/cleaned/audio_segments/{youtube_id}_segments_meta.json
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from config.settings import settings
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _check_ffmpeg() -> None:
    """Raise RuntimeError if ffmpeg is not findable on the system PATH."""
    if not shutil.which("ffmpeg"):
        raise RuntimeError(
            "ffmpeg is required for audio segmentation. "
            "Install with: brew install ffmpeg  (macOS)"
        )


class AudioSegmenter:
    """Splits podcast MP3 files into fixed-length overlapping segments.

    Attributes:
        audio_dir: Source directory containing full-episode MP3 files.
        segments_dir: Output directory for segment files and metadata.
        segment_ms: Target segment length in milliseconds.
        overlap_ms: Overlap between consecutive segments in milliseconds.
        checkpoint: Tracks which episodes have been segmented.
    """

    def __init__(self) -> None:
        """Initialise paths and timing parameters from settings."""
        _check_ffmpeg()
        self.audio_dir = settings.raw_dir / "audio"
        self.segments_dir = settings.cleaned_dir / "audio_segments"
        self.segments_dir.mkdir(parents=True, exist_ok=True)
        self.segment_ms = settings.audio_segment_seconds * 1000
        self.overlap_ms = settings.audio_overlap_seconds * 1000
        self.checkpoint = Checkpoint("audio_segment")

    def segment_one(self, episode: dict[str, Any]) -> list[dict[str, Any]]:
        """Segment a single episode's MP3 into overlapping clips.

        Args:
            episode: Episode metadata dict (must include ``youtube_id``,
                ``title``, ``channel``, ``published_at``).

        Returns:
            List of segment metadata dicts conforming to Appendix A schema.
            Empty list if the source MP3 does not exist.
        """
        from pydub import AudioSegment as PydubSegment

        vid_id = episode["youtube_id"]
        mp3_path = self.audio_dir / f"{vid_id}.mp3"

        if not mp3_path.exists():
            logger.warning("source audio not found, skipping", youtube_id=vid_id)
            return []

        logger.info("segmenting audio", youtube_id=vid_id, path=str(mp3_path))
        audio = PydubSegment.from_mp3(str(mp3_path))
        total_ms = len(audio)

        step_ms = self.segment_ms - self.overlap_ms
        date = episode.get("published_at", "")[:10]

        segment_meta: list[dict[str, Any]] = []
        seg_idx = 0
        start_ms = 0

        while start_ms < total_ms:
            end_ms = min(start_ms + self.segment_ms, total_ms)
            segment = audio[start_ms:end_ms]

            filename = f"{vid_id}_seg_{seg_idx:03d}.mp3"
            out_path = self.segments_dir / filename
            segment.export(str(out_path), format="mp3")

            meta = {
                "segment_id": f"podcast_audio_{vid_id}_{seg_idx:03d}",
                "source_type": "podcast_audio",
                "source_name": f"{episode.get('channel', '')} — {episode.get('title', '')}",
                "source_id": vid_id,
                "date": date,
                "modality": "audio",
                "teams": "",
                "players": "",
                "gameweek": None,
                "chunk_index": seg_idx,
                "total_chunks": -1,  # Will be updated after loop
                "start_seconds": round(start_ms / 1000, 1),
                "end_seconds": round(end_ms / 1000, 1),
                "duration_seconds": round((end_ms - start_ms) / 1000, 1),
                "filename": filename,
            }
            segment_meta.append(meta)
            seg_idx += 1

            if end_ms >= total_ms:
                break
            start_ms += step_ms

        # Back-fill total_chunks now that we know the count
        for m in segment_meta:
            m["total_chunks"] = seg_idx

        # Save metadata JSON
        meta_path = self.segments_dir / f"{vid_id}_segments_meta.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(segment_meta, f, indent=2, ensure_ascii=False)

        logger.info(
            "segmentation complete",
            youtube_id=vid_id,
            segments=seg_idx,
            total_duration_s=round(total_ms / 1000),
        )
        return segment_meta

    def segment_all(self, episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Segment all downloaded episode audio files.

        Args:
            episodes: Episode manifest from Agent 1.

        Returns:
            Flat list of all segment metadata dicts.
        """
        all_meta: list[dict[str, Any]] = []
        for episode in episodes:
            vid_id = episode["youtube_id"]
            if self.checkpoint.is_completed(vid_id):
                logger.info("already segmented, skipping", youtube_id=vid_id)
                continue
            meta = self.segment_one(episode)
            if meta:
                all_meta.extend(meta)
                self.checkpoint.mark_completed(vid_id)

        logger.info("audio segmentation complete", total_segments=len(all_meta))
        return all_meta


def run_audio_segmentation(episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Orchestrate audio segmentation for all episodes.

    Args:
        episodes: Episode manifest from Agent 1.

    Returns:
        All segment metadata dicts.
    """
    segmenter = AudioSegmenter()
    return segmenter.segment_all(episodes)
