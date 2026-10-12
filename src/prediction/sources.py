"""Reconstruct prediction inputs from reviewed original results, without downloads."""

from config.sources import inspect_source, load_sources
from src.ingest.historical_matches import normalize_matches, validate_coverage
from src.ingest.match_history import load_history_sources, raw_path


def load_results(output):
    sources = load_history_sources()
    sources += [s for s in load_sources("2025-26") if s.id == "matches"]
    if sorted(s.season for s in sources) != ["2022-23", "2023-24", "2024-25", "2025-26"]:
        raise ValueError("Expected the four reviewed result seasons")
    rows, references = [], []
    for source in sources:
        path = (
            raw_path(output, source.season)
            if source.season != "2025-26"
            else (output / "raw/mvp/2025-26/matches.csv")
        )
        content = path.read_bytes()
        inspect_source(source, content)
        matches = normalize_matches(content, source.season, source.id)
        if not validate_coverage(matches)["valid"]:
            raise ValueError("Incomplete result season")
        reference = {
            "season": source.season,
            "url": source.url,
            "revision": source.revision,
            "sha256": source.sha256,
            "publisher": source.publisher,
        }
        references.append(reference)
        rows.extend({**m, "source": {**reference, "row": m["source_row"]}} for m in matches)
    return sorted(rows, key=lambda m: (m["date"], m["id"])), references
