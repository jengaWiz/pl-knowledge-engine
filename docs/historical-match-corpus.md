# Historical Premier League match corpus

The optional historical collector adds 2022–23, 2023–24 and 2024–25 match evidence
from reviewed Football-Data CSVs. Each season contains 380 completed fixtures,
20 clubs and both home/away meetings. It keeps these seasons separate from the
2025–26 MVP; current player coverage and the default demo index are unchanged.

```bash
uv run --locked python scripts/collect_history.py
# Or use --output /path/to/data to isolate the corpus.
```

`config/historical_sources.json` pins URLs, content checksums, revisions, required
columns and publisher reuse terms. A changed public download or corrupt cache
fails validation; source-contract changes require review. The shared downloader
limits each source to 20 MB and retries transient failures at most three times.
Successful downloads remain cached after failed runs. No paid API is used.

The collector independently validates season dates, scores, canonical team names,
fixture completeness and statistics missingness. It derives one natural text
retrieval document per match, retaining its source row, original file ID, URL,
checksum and revision. Missing metrics stay unknown. Whole CSVs count as original
assets; descriptions are derived seasonal assets with actual fixture dates.
Betting fields are not rendered into documents or treated as prediction features.

The default limits are 2,000 documents and 2 MB of generated UTF-8 text. Change
them with `--document-budget` and `--byte-budget`. Manifest metadata and original
CSV bytes are separate from this text payload budget. Collection is locked and
accepted manifests are withdrawn during an incomplete or failed run. A retry
reuses verified source caches and regenerates deterministic documents.

Outputs remain ignored local data:

- `raw/history/<season>/matches.csv`: pinned original bytes.
- `cleaned/history/<season>/matches.jsonl`: normalized fixtures by season.
- `cleaned/history/assets.jsonl` and `documents.jsonl`: shared corpus contracts.
- `reports/history/corpus.json`: per-season coverage, missingness, profile and hashes.

`load_history_documents(output)` verifies the report and reconstructs accepted
artifacts from the pinned raw files before downstream use. Changed normalized
rows, text, source configuration or manifests fail this gate.

The live verified run produced **1,140 distinct match documents** from three
original CSV files. Every season passed completeness and had no missing values
in the twelve optional match-statistic fields used by the normalizer. This adds
useful historical evidence; it does not yet create embeddings, load graph nodes
or train a prediction model. Earlier seasons can support later temporal
training/validation/test splits, whose leakage checks and measured results remain
separate work. These aggregate statistics do not establish tactical events such
as pressing or passing sequences.

Attribution and the publisher's reuse terms are preserved; no SPDX license is
invented. Raw data is not published in this repository.
