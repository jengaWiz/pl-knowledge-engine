"""
Gemini Embedding 2 client.

Wraps the google-genai SDK to produce 3072-dimensional embedding vectors
for text, image, audio, and video inputs.

Usage:
    client = GeminiEmbedder()
    vector = client.embed_text("Liverpool won 2-1 at Anfield")
    vector = client.embed_image(Path("data/raw/images/players/salah.png"))
    vector = client.embed_audio(Path("data/cleaned/audio_segments/abc_seg_000.mp3"))
    vector = client.embed_video(Path("data/raw/video/match123_highlights.mp4"))
"""
from __future__ import annotations

from pathlib import Path

from google import genai
from google.genai import types

from config.settings import settings
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)

_MIME_MAP: dict[str, str] = {
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".mp4": "video/mp4",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
}


class GeminiEmbedder:
    """Produces Gemini Embedding 2 vectors for all supported modalities.

    Attributes:
        client: Authenticated google.genai Client instance.
        model: Embedding model ID from settings.
    """

    def __init__(self) -> None:
        """Initialise the Gemini client using the API key from settings."""
        self.client = genai.Client(api_key=settings.gemini_api_key)
        self.model = settings.gemini_model
        logger.info("gemini embedder initialised", model=self.model)

    @retry(max_attempts=5, base_delay=1.0)
    def embed_text(self, text: str) -> list[float]:
        """Embed a single text string.

        Args:
            text: Input text (up to ~8192 tokens for Gemini Embedding 2).

        Returns:
            List of 3072 floats.
        """
        response = self.client.models.embed_content(
            model=self.model,
            contents=text,
        )
        return response.embeddings[0].values

    @retry(max_attempts=5, base_delay=1.0)
    def embed_texts_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of text strings in a single API call.

        Sends all texts together via batchEmbedContents, consuming only one
        request against the rate limit regardless of batch size.

        Args:
            texts: List of input strings (up to 100 items per call).

        Returns:
            List of embedding vectors, one per input text.
        """
        response = self.client.models.embed_content(
            model=self.model,
            contents=texts,
        )
        return [e.values for e in response.embeddings]

    @retry(max_attempts=5, base_delay=1.0)
    def embed_image(self, image_path: Path) -> list[float]:
        """Embed an image file.

        Args:
            image_path: Path to a PNG or JPEG image file.

        Returns:
            List of 3072 floats.
        """
        mime = _MIME_MAP.get(image_path.suffix.lower(), "image/png")
        image_bytes = image_path.read_bytes()
        part = types.Part.from_bytes(data=image_bytes, mime_type=mime)
        response = self.client.models.embed_content(
            model=self.model,
            contents=part,
        )
        return response.embeddings[0].values

    @retry(max_attempts=5, base_delay=1.0)
    def embed_audio(self, audio_path: Path) -> list[float]:
        """Embed an audio file (MP3 or WAV).

        Args:
            audio_path: Path to an MP3 or WAV audio file.

        Returns:
            List of 3072 floats.
        """
        mime = _MIME_MAP.get(audio_path.suffix.lower(), "audio/mpeg")
        audio_bytes = audio_path.read_bytes()
        part = types.Part.from_bytes(data=audio_bytes, mime_type=mime)
        response = self.client.models.embed_content(
            model=self.model,
            contents=part,
        )
        return response.embeddings[0].values

    @retry(max_attempts=5, base_delay=1.0)
    def embed_video(self, video_path: Path) -> list[float]:
        """Embed a video file (MP4).

        Args:
            video_path: Path to an MP4 video file.

        Returns:
            List of 3072 floats.
        """
        video_bytes = video_path.read_bytes()
        part = types.Part.from_bytes(data=video_bytes, mime_type="video/mp4")
        response = self.client.models.embed_content(
            model=self.model,
            contents=part,
        )
        return response.embeddings[0].values
