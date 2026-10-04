"""Pinned, attributed source contracts for the historical MVP."""

import csv
import hashlib
import io
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from importlib.resources import files
from typing import Any

from config.season import validate_season_dates


@dataclass(frozen=True)
class SourceSpec:
    id: str
    season: str
    url: str
    publisher: str
    reference_url: str
    reuse_terms: str
    revision: str
    sha256: str
    required_columns: list[str]
    schema_note: str


def load_sources(season: str) -> list[SourceSpec]:
    """Reject an unsupported season rather than choosing another year's files."""
    records = json.loads(files("config").joinpath("sources.json").read_text())
    sources = [SourceSpec(**item) for item in records if item["season"] == season]
    if not sources:
        raise ValueError(f"No verified source contracts for season {season}")
    return sources


def inspect_source(source: SourceSpec, content: bytes) -> dict[str, Any]:
    """Check content identity, schema and available season evidence."""
    checksum = hashlib.sha256(content).hexdigest()
    if checksum != source.sha256:
        raise ValueError(f"{source.id}: checksum changed; review the source before accepting it")
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    missing = set(source.required_columns) - set(reader.fieldnames or [])
    if missing:
        raise ValueError(f"{source.id}: missing columns {sorted(missing)}")
    rows = [row for row in reader if any(row.values())]
    if not rows:
        raise ValueError(f"{source.id}: empty dataset")
    report = {**asdict(source), "rows": len(rows), "bytes": len(content)}
    if source.id == "matches":
        if any(row["Div"] != "E0" for row in rows):
            raise ValueError("Match source contains a non-Premier-League competition")
        dates = [datetime.strptime(row["Date"], "%d/%m/%Y").date().isoformat() for row in rows]
        validate_season_dates(dates, source.season)
        report["date_range"] = [min(dates), max(dates)]
        report["teams"] = sorted({row[field] for row in rows for field in ("HomeTeam", "AwayTeam")})
    elif source.id == "teams":
        from config.season import resolve_focus_ids

        report["focus_team_ids"] = resolve_focus_ids(rows, {"Aston Villa", "Liverpool"})
    return report
