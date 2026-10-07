# Structured football evidence acceptance

Structured match/player records and source-backed text are the primary corpus.
Media remains an optional separate mode. The default acceptance target is 2,500
unique retrieval documents, including text; it does not require images, audio or
video and does not describe structured/text evidence as multimodal.

## Prepare and verify

```bash
make mvp
make prepare-text
make collect-history
uv run --locked python scripts/accept_corpus.py --require-target
# Optional original media corpus, independently stored:
uv run --locked python scripts/accept_corpus.py --mode multimodal
```

All stages use the same data directory. For custom locations, pass `--output` to
each script. The structured route requires no FFmpeg, Gemini or paid API.
`make accept-corpus` now selects the structured route by default.

The structured gate combines the accepted 2025–26 statistical text with pinned
historical match descriptions. Both source gates must pass; missing history is
not silently skipped. Earlier seasons must precede the primary season, preventing
a future dataset from being mislabeled as historical input. This check alone is
not a model leakage audit: temporal feature construction and evaluation splits
remain separate work.

Every document retains its season, date, canonical IDs and original row/revision
references. The report lists all included seasons and their separate date ranges.
Its `season` field identifies the primary MVP season, not the sole data coverage.
Consumers must filter by requested season rather than mix historical fixtures
into 2025–26 answers. The new evidence is not yet queried by the analyst.

## Current verified preparation

| Measure | Result |
| --- | ---: |
| Unique structured/text documents | 2,682 |
| Match documents across four seasons | 1,520 |
| Positive-minute player appearances, 2025–26 only | 1,162 |
| Original CSV source files | 120 |
| Derived text assets | 2,682 |
| Generated UTF-8 text bytes | 506,823 |
| Duplicate document content | 0 |
| Embeddings created by acceptance | 0 |

Included seasons: 2022–23, 2023–24, 2024–25 and 2025–26, with 380 matches in each.
Detailed player evidence still covers Aston Villa and Liverpool in 2025–26.
Original source files and generated descriptions are counted separately.

The structured report has `target_met: true` and `status: prepared_target_met`.
Its policy requires 2,500 unique documents and text coverage. The contract's
separate `counts.multimodal_target_met` remains false for text-only evidence;
it does not control the structured policy. `--require-target` uses the stored
mode-specific policy, not an assumption about four modalities.

This completes the preparation count target. A [separate local ONNX index](local-evidence-index.md)
now embeds this corpus; hybrid retrieval, prediction evaluation and Gemini model/endpoint
acceptance remain unfinished. The default demo remains the verified
2025–26 MVP with its existing 457 ONNX-indexed summaries and graph.

## Output and migration

Structured outputs use a separate namespace:

- `cleaned/evidence/<primary-season>/corpus_assets.jsonl`
- `cleaned/evidence/<primary-season>/corpus_documents.jsonl`
- `reports/evidence/<primary-season>/corpus.json`

Optional multimodal outputs remain under `cleaned/multimodal` and
`reports/multimodal`. Its original target still requires all four modalities and
2,500 documents; the reviewed media/text sample remains below that target.
The mode is explicit in every report. Changing modes cannot overwrite the other
mode's accepted artifacts.

Acceptance schema v2 supersedes v1 reports; rerun acceptance for the desired mode.
Source-stage files remain reusable. `load_corpus(output, season)` validates the
structured mode by default; pass `mode="multimodal"` for the optional corpus.
Both modes snapshot source manifests, invoke original integrity gates, reject
moving inputs/conflicting identities, deduplicate content and reproduce combined
manifests before loading. Budgets retain their previous defaults: 5,000 document
records and 150 MB of unique payloads; metadata and source files are separate.

These artifacts remain ignored local data. They provide aggregate football
evidence, not event-level proof of pressing or passing sequences. Prediction
claims require a separately evaluated baseline with temporal leakage checks.
