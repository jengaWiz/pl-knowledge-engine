"""
Transcript cleaner.

Cleans raw auto-generated YouTube transcript text, fixing punctuation,
capitalisation, player/team name spellings, and paragraph structure.

Two modes:
- **LLM mode** (uses ``GEMINI_API_KEY``): sends ~2000-word chunks to
  Gemini for high-quality cleaning.
- **Regex fallback**: applies rule-based fixes with no external dependency.

Output: data/cleaned/transcripts/{youtube_id}_cleaned.txt
"""
from __future__ import annotations

import re
from typing import Any

from config.settings import settings
from src.utils.checkpoint import Checkpoint
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Known name spellings for regex-based correction
# ---------------------------------------------------------------------------
PLAYER_NAMES: list[tuple[str, str]] = [
    (r"\bsalah\b", "Salah"),
    (r"\bvan dijk\b", "Van Dijk"),
    (r"\bwatkins\b", "Watkins"),
    (r"\bemery\b", "Emery"),
    (r"\bslot\b", "Slot"),
    (r"\balisson\b", "Alisson"),
    (r"\bdiaz\b", "Díaz"),
    (r"\bnuñez\b", "Núñez"),
    (r"\bgakpo\b", "Gakpo"),
    (r"\bmac allister\b", "Mac Allister"),
    (r"\btsimikas\b", "Tsimikas"),
    (r"\bcoutinho\b", "Coutinho"),
    (r"\btraoré\b", "Traoré"),
    (r"\bdigne\b", "Digne"),
    (r"\bcash\b", "Cash"),
    (r"\byoung\b", "Young"),
    (r"\btylor mings\b", "Tyrone Mings"),
]

TEAM_NAMES: list[tuple[str, str]] = [
    (r"\baston villa\b", "Aston Villa"),
    (r"\bliverpool\b", "Liverpool"),
    (r"\bthe villa\b", "Aston Villa"),
    (r"\bthe reds\b", "Liverpool"),
    (r"\bvilla\b", "Villa"),
]

CLEANING_PROMPT = """\
You are cleaning an auto-generated YouTube transcript from a Premier League \
football podcast.

Fix the following issues:
1. Add proper punctuation and capitalisation
2. Fix player/manager name spellings (reference list: Mohamed Salah, Virgil van \
Dijk, Ollie Watkins, Unai Emery, Arne Slot, Alisson Becker, Luis Díaz, Darwin \
Núñez, Cody Gakpo, Alexis Mac Allister)
3. Fix team name spellings (Aston Villa, Liverpool)
4. Remove filler words and false starts (um, uh, you know, like) only when they \
don't affect meaning
5. Add paragraph breaks at natural topic transitions
6. Do NOT change the meaning or add information that isn't there

Return ONLY the cleaned text, no commentary.

Raw transcript:
{raw_text}"""


# ---------------------------------------------------------------------------
# LLM cleaner (Gemini)
# ---------------------------------------------------------------------------


@retry(max_attempts=3, base_delay=2.0)
def _call_gemini(chunk: str) -> str:
    """Send a transcript chunk to Gemini for LLM-based cleaning.

    Args:
        chunk: Raw transcript text (approx. 2000 words).

    Returns:
        Cleaned text returned by the model.
    """
    from google import genai

    client = genai.Client(api_key=settings.gemini_api_key)
    response = client.models.generate_content(
        model=settings.gemini_text_model,
        contents=CLEANING_PROMPT.format(raw_text=chunk),
    )
    return response.text


def _llm_clean(raw_text: str, chunk_words: int = 2000) -> str:
    """Clean a full transcript via Gemini in word-chunked batches.

    Args:
        raw_text: Full raw transcript string.
        chunk_words: Approximate word count per LLM batch.

    Returns:
        Full cleaned transcript string.
    """
    words = raw_text.split()
    chunks = [
        " ".join(words[i : i + chunk_words])
        for i in range(0, len(words), chunk_words)
    ]

    cleaned_chunks: list[str] = []
    for i, chunk in enumerate(chunks):
        logger.info(
            "cleaning chunk with llm",
            chunk=i + 1,
            total=len(chunks),
            words=len(chunk.split()),
        )
        cleaned_chunks.append(_call_gemini(chunk))

    return "\n\n".join(cleaned_chunks)


# ---------------------------------------------------------------------------
# Regex fallback cleaner
# ---------------------------------------------------------------------------


def _regex_clean(raw_text: str) -> str:
    """Apply rule-based cleaning to a raw transcript string.

    Steps:
    1. Collapse multiple spaces.
    2. Capitalise the first letter of each sentence.
    3. Apply known player/team name corrections (case-insensitive).
    4. Remove common filler words.
    5. Clean up excess spaces.

    Args:
        raw_text: Raw concatenated transcript text.

    Returns:
        Lightly cleaned transcript string.
    """
    text = re.sub(r"\s+", " ", raw_text).strip()

    text = re.sub(
        r"(^|[.!?]\s+)([a-z])",
        lambda m: m.group(1) + m.group(2).upper(),
        text,
    )

    for pattern, replacement in PLAYER_NAMES:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    for pattern, replacement in TEAM_NAMES:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    fillers = r"\b(um+|uh+|er+|hmm+|you know|i mean|sort of|kind of|like)\b,?\s*"
    text = re.sub(fillers, " ", text, flags=re.IGNORECASE)

    text = re.sub(r"\s+", " ", text).strip()

    return text


# ---------------------------------------------------------------------------
# Main cleaner class
# ---------------------------------------------------------------------------


class TranscriptCleaner:
    """Cleans raw transcript files for all downloaded episodes.

    Attributes:
        raw_dir: Directory containing raw transcript text files.
        cleaned_dir: Output directory for cleaned transcripts.
        checkpoint: Tracks which episodes have been cleaned.
        use_llm: Always True — Gemini key is always available.
    """

    def __init__(self) -> None:
        """Set up cleaner with paths from settings."""
        self.raw_dir = settings.raw_dir / "transcripts"
        self.cleaned_dir = settings.cleaned_dir / "transcripts"
        self.cleaned_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint = Checkpoint("transcript_clean")
        self.use_llm = bool(settings.gemini_api_key)
        logger.info(
            "transcript cleaner initialised",
            mode="llm" if self.use_llm else "regex",
        )

    def clean_one(self, youtube_id: str) -> bool:
        """Clean the transcript for a single episode.

        Args:
            youtube_id: YouTube video ID — used to locate the raw file.

        Returns:
            True if cleaning succeeded, False if the raw file doesn't exist.
        """
        raw_path = self.raw_dir / f"{youtube_id}_raw.txt"
        if not raw_path.exists():
            logger.warning("raw transcript not found", youtube_id=youtube_id)
            return False

        raw_text = raw_path.read_text(encoding="utf-8")

        if self.use_llm:
            cleaned = _llm_clean(raw_text)
        else:
            cleaned = _regex_clean(raw_text)

        out_path = self.cleaned_dir / f"{youtube_id}_cleaned.txt"
        out_path.write_text(cleaned, encoding="utf-8")
        logger.info(
            "transcript cleaned",
            youtube_id=youtube_id,
            mode="llm" if self.use_llm else "regex",
            input_words=len(raw_text.split()),
            output_words=len(cleaned.split()),
        )
        return True

    def clean_all(self, episodes: list[dict[str, Any]]) -> None:
        """Clean transcripts for all episodes in the manifest.

        Args:
            episodes: List of episode metadata dicts with ``youtube_id`` keys.
        """
        for episode in episodes:
            vid_id = episode["youtube_id"]
            if self.checkpoint.is_completed(vid_id):
                logger.info("already cleaned, skipping", youtube_id=vid_id)
                continue
            if self.clean_one(vid_id):
                self.checkpoint.mark_completed(vid_id)

        logger.info("transcript cleaning complete", total=len(episodes))


def run_transcript_cleaning(episodes: list[dict[str, Any]]) -> None:
    """Orchestrate transcript cleaning for all episodes.

    Args:
        episodes: Episode manifest list from the YouTube search stage.
    """
    cleaner = TranscriptCleaner()
    cleaner.clean_all(episodes)
