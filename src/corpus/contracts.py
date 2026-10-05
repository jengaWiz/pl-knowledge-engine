"""Keep original source assets, derived media and retrieval chunks distinguishable."""

import hashlib
import math
from collections import Counter
from datetime import date
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from config.season import season_bounds, validate_season_dates

Modality = Literal["text", "image", "audio", "video"]
MIME_TYPES = {
    "text": {"text/plain", "text/html", "application/json", "text/csv"},
    "image": {"image/jpeg", "image/png", "image/webp", "image/svg+xml"},
    "audio": {"audio/ogg", "audio/mpeg", "audio/wav", "audio/flac"},
    "video": {"video/mp4", "video/webm", "video/ogg"},
}


def content_id(kind: str, checksum: str) -> str:
    return f"{kind}:{checksum}"


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class Asset(BaseModel):
    """One downloaded source or explicitly derived asset; unknown dates stay null."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    modality: Modality
    mime_type: str
    bytes: int = Field(gt=0)
    source_url: str
    reference_url: str
    publisher: str = Field(min_length=1)
    attribution: str = Field(min_length=1)
    license_name: str = Field(min_length=1)
    license_url: str
    published_at: date | None = None
    event_date: date | None = None
    scope: Literal["seasonal", "background"]
    season: str | None = None
    entity_ids: tuple[str, ...] = ()
    parent_asset_id: str | None = None

    @field_validator("source_url", "reference_url", "license_url")
    @classmethod
    def public_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("Provenance requires an HTTPS URL without embedded credentials")
        return value

    @model_validator(mode="after")
    def identity_and_scope(self):
        if self.id != content_id("asset", self.sha256):
            raise ValueError("Asset ID must derive from its content checksum")
        if self.mime_type not in MIME_TYPES[self.modality]:
            raise ValueError("MIME type does not match declared modality")
        if self.parent_asset_id == self.id:
            raise ValueError("An asset cannot derive from itself")
        if self.scope == "seasonal":
            if self.season is None or self.event_date is None:
                raise ValueError("Seasonal evidence requires a season and event date")
            season_bounds(self.season)
            validate_season_dates([self.event_date.isoformat()], self.season)
        elif self.season is not None:
            raise ValueError("Background evidence cannot imply seasonal coverage")
        return self


class Document(BaseModel):
    """An indexed unit with a parent and a reproducible extraction location."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str
    asset_id: str
    modality: Modality
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    extraction_version: str = Field(min_length=1)
    start: float = Field(ge=0)
    end: float = Field(gt=0)
    coordinate: Literal["characters", "seconds", "whole_image"]

    @model_validator(mode="after")
    def extraction(self):
        if not math.isfinite(self.start) or not math.isfinite(self.end) or self.end <= self.start:
            raise ValueError("Extraction range must be finite and nonempty")
        expected = {"text": "characters", "image": "whole_image"}.get(self.modality, "seconds")
        if self.coordinate != expected:
            raise ValueError("Extraction coordinate does not match modality")
        if self.coordinate == "characters" and (self.start % 1 or self.end % 1):
            raise ValueError("Character offsets must be integers")
        if self.coordinate == "whole_image" and (self.start, self.end) != (0, 1):
            raise ValueError("Whole-image coordinates must be 0..1")
        expected_id = content_id(
            "document",
            sha256(
                f"{self.asset_id}|{self.modality}|{self.start:g}|{self.end:g}|"
                f"{self.extraction_version}|{self.sha256}".encode()
            ),
        )
        if self.id != expected_id:
            raise ValueError("Document ID must derive from parent, extraction and content")
        return self


def document_id(asset_id, modality, start, end, extraction_version, checksum):
    return content_id(
        "document",
        sha256(f"{asset_id}|{modality}|{start:g}|{end:g}|{extraction_version}|{checksum}".encode()),
    )


def count_corpus(assets: list[Asset], documents: list[Document]) -> dict:
    """Reject broken provenance and report deduplicated assets/chunks separately."""
    by_id = {asset.id: asset for asset in assets}
    if len(by_id) != len(assets) or len({doc.id for doc in documents}) != len(documents):
        raise ValueError("Duplicate asset or document IDs")
    for asset in assets:
        seen = {asset.id}
        current = asset
        while current.parent_asset_id:
            parent = current.parent_asset_id
            if parent not in by_id or parent in seen:
                raise ValueError("Missing parent asset or cyclic derivation")
            seen.add(parent)
            current = by_id[parent]
    for doc in documents:
        if doc.asset_id not in by_id or by_id[doc.asset_id].modality != doc.modality:
            raise ValueError("Document parent is absent or has a different modality")
    unique = {}
    for doc in documents:
        unique.setdefault((doc.modality, doc.sha256), doc)
    sources = [asset for asset in assets if asset.parent_asset_id is None]
    modalities = sorted(MIME_TYPES)
    doc_counts = Counter(doc.modality for doc in unique.values())
    source_counts = Counter(asset.modality for asset in sources)
    return {
        "source_assets": len(sources),
        "derived_assets": len(assets) - len(sources),
        "retrieval_documents": len(unique),
        "duplicate_document_content": len(documents) - len(unique),
        "source_assets_by_modality": {key: source_counts[key] for key in modalities},
        "documents_by_modality": {key: doc_counts[key] for key in modalities},
        "multimodal_target_met": len(unique) >= 2500 and all(doc_counts[key] for key in modalities),
    }
