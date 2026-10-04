"""
Central configuration loaded from environment variables.
All modules import from here — never read .env directly.
"""

from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings

from config.season import season_bounds


class Settings(BaseSettings):
    # API Keys
    gemini_api_key: str = ""
    youtube_api_key: str = ""
    balldontlie_api_key: str = ""

    # Gemini models
    gemini_text_model: str = "gemini-2.0-flash"

    # Neo4j
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""

    # Historical MVP scope; source adapters must prove this season.
    season: str = "2025-26"

    @field_validator("season")
    @classmethod
    def validate_season(cls, value: str) -> str:
        season_bounds(value)
        return value

    def require_credentials(self, *names: str) -> None:
        missing = [name.upper() for name in names if not getattr(self, name).strip()]
        if missing:
            raise ValueError("This feature requires: " + ", ".join(missing))

    # Paths (relative to project root)
    data_dir: Path = Path("data")
    raw_dir: Path = Path("data/raw")
    cleaned_dir: Path = Path("data/cleaned")
    embedded_dir: Path = Path("data/embedded")
    checkpoint_dir: Path = Path("data/checkpoints")

    # Embedding config
    gemini_model: str = "gemini-embedding-2-preview"
    embedding_dimensions: int = 3072
    embedding_batch_size: int = 100
    embedding_delay_seconds: float = 1.0

    # Chunking config
    chunk_size_words: int = 400
    chunk_overlap_words: int = 100
    audio_segment_seconds: int = 75
    audio_overlap_seconds: int = 10

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
