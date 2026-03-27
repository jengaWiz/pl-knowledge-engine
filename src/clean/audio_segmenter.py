"""
Audio segmenter.

Splits full podcast MP3 files into overlapping fixed-length segments
suitable for Gemini Embedding 2 (requires ffmpeg via pydub).

Output:
    data/cleaned/audio_segments/{youtube_id}_seg_{i:03d}.mp3
    data/cleaned/audio_segments/{youtube_id}_segments_meta.json
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config.settings import settings
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger

logger = get_logger(__name__)


class AudioSegmenter:
    """Splits podcast MP3 episodes into overlapping segments.

    Attributes:
        audio_dir: Directory containing full-episode MP3 files.
        output_dir: Directory for segment MP3 files and metadata.
        segment_ms: Segment length in milliseconds.
        overlap_ms: Overlap between consecutive segments in milliseconds.
        step_ms: Step size (segment_ms - overlap_ms).
        checkpoint: Stage checkpoint for resumability.
    """

    def __init__(self) -> None:
        """Initialise from settings."""
        self.audio_dir = settings.raw_dir / "audio"
        self.output_dir = settings.cleaned_dir / "audio_segments"
        self.segment_ms: int = settings.audio_segment_seconds * 1000
        self.overlap_ms: int = settings.audio_overlap_seconds * 1000
        self.step_ms: int = self.segment_ms - self.overlap_ms
        self.checkpoint = Checkpoint("audio_segment")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def segment_episode(self, episode: dict[str, Any]) -> list[dict[str, Any]]:
        """Segment a single episode's MP3 into overlapping clips.

        If the episode has already been segmented (checkpoint hit), loads and
        returns the existing metadata JSON rather than re-processing.

        Args:
            episode: Episode metadata dict (must include ``youtube_id``,
                ``title``, ``channel``, ``published_at``).

        Returns:
            List of segment metadata dicts. Empty list if MP3 absent.
        """
        from pydub import AudioSegment  # import here so module loads without ffmpeg

        vid_id = episode["youtube_id"]
        mp3_path = self.audio_dir / f"{vid_id}.mp3"

        if not mp3_path.exists():
            logger.warning("source audio not found, skipping", youtube_id=vid_id)
            return []

        # Load existing metadata if already checkpointed
        if self.checkpoint.is_completed(vid_id):
            meta_path = self.output_dir / f"{vid_id}_segments_meta.json"
            if meta_path.exists():
                logger.info("already segmented, loading metadata", youtube_id=vid_id)
                with open(meta_path, encoding="utf-8") as f:
                    return json.load(f)
            logger.info("checkpoint hit but metadata missing, re-segmenting", youtube_id=vid_id)

        logger.info("segmenting", youtube_id=vid_id)
        audio = AudioSegment.from_mp3(str(mp3_path))
        total_ms = len(audio)
        date = episode.get("published_at", "")[:10]
        channel = episode.get("channel", "")
        title = episode.get("title", "")

        segments_meta: list[dict[str, Any]] = []
        i = 0
        start = 0

        while start < total_ms:
            end = min(start + self.segment_ms, total_ms)
            clip = audio[start:end]

            filename = f"{vid_id}_seg_{i:03d}.mp3"
            clip.export(str(self.output_dir / filename), format="mp3")

            segments_meta.append(
                {
                    "segment_id": f"podcast_audio_{vid_id}_{i:03d}",
                    "source_id": vid_id,
                    "channel": channel,
                    "title": title,
                    "date": date,
                    "modality": "audio",
                    "segment_index": i,
                    "start_seconds": round(start / 1000, 1),
                    "end_seconds": round(end / 1000, 1),
                    "file_path": str(self.output_dir / filename),
                }
            )
            i += 1

            if end >= total_ms:
                break
            start += self.step_ms

        # Save metadata JSON
        meta_path = self.output_dir / f"{vid_id}_segments_meta.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(segments_meta, f, indent=2, ensure_ascii=False)

        self.checkpoint.mark_completed(vid_id)
        logger.info(
            "segmentation complete",
            youtube_id=vid_id,
            segments=i,
            duration_s=round(total_ms / 1000),
        )
        return segments_meta

    def segment_all(self, episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Segment all episodes and return combined metadata.

        Args:
            episodes: Episode manifest from Agent 1.

        Returns:
            Flat list of all segment metadata dicts.
        """
        all_meta: list[dict[str, Any]] = []
        for episode in episodes:
            all_meta.extend(self.segment_episode(episode))
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
