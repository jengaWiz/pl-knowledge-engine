# Local structured evidence index

This optional index connects the accepted structured corpus to persistent Chroma
using the existing local all-MiniLM-L6-v2 ONNX model. It is a separately versioned
local retrieval stage, not Gemini Embedding 2 or completed hybrid analysis.

## Build and query

Prepare the structured evidence first, then:

```bash
uv run --locked python scripts/accept_corpus.py --require-target
uv run --locked python scripts/index_evidence.py
```

Pass `--output /path/to/data` and `--season` when using a custom directory or
primary season. The model has 384 dimensions and a pinned archive checksum. Its
initial public model download may be needed if it is not already cached; no API
key or billed provider is involved. The verified local run reused the cached
model. Only checksum-verified text is accepted; media is not sent through this
text embedding space.

```python
from pathlib import Path
from src.store.evidence_index import search

hits = search(
    Path("data"), "2025-26", "Liverpool home match against Bournemouth",
    evidence_season="2022-23", kind="match", limit=5,
)
```

An explicit `evidence_season` is required. Optional exact filters are `kind`
(`match` or `appearance`), canonical `player_id` and canonical `match_id`.
Unknown seasons fail instead of falling back to another season; filters with no
matching evidence return an empty list. Queries are bounded to 4,000 characters
and 1–50 results. Returned text includes source metadata, original row/revision
references and canonical IDs for later graph expansion.

## Persistence and integrity

The model/profile/dataset hash determines the collection identity. The profile
includes dimensions, cosine distance, normalization and accepted evidence hash.
Chroma lives under `stores/evidence_chroma`, independent of `stores/chroma` and
the default 457-summary MVP index. Changed evidence creates another owned
collection; old versions and unrelated collections are retained.

Every batch of 32 records is checkpointed only after stored text, metadata and
vectors pass verification. Vectors must have the expected dimensions, finite
values and nonzero norms. Inputs are normalized in float32; the cosine engine's
minor normalization rounding is checked with a bounded numerical tolerance.
Checkpoints record exact stored-vector hashes, which are rechecked on resume and
query. Interrupted runs reuse valid completed batches; corrupt checkpoints or
stored content fail rather than silently overwriting accepted evidence.

The report is invalid while a build runs or after failure. Every query rechecks
the accepted corpus, model/profile, complete stored record count, text/provenance,
all batch checkpoints and stored-vector hashes before retrieval. This favors
integrity over minimum latency in the prototype; full-index checks add work to
each query and may need a separately validated optimization at larger scale.

Outputs remain ignored local artifacts:

- `stores/evidence_chroma/`: persistent ONNX vectors and source-linked text.
- `checkpoints/evidence_index/<dataset-id>/batch_<offset>.json`: resumable batch hashes.
- `reports/evidence/<primary-season>/index.json`: readiness, model profile, counts,
  per-season records and aggregate vector checksum.

The verified local corpus contains 2,682 records and 2,682 unique documents.
Counts distinguish traceable records from unique content if later sources add
repeated descriptions; repeated content must not inflate the scale claim.

The analyst API and React interface still use the established MVP. Evidence
indexing does not train a prediction model, expose new endpoints, load evidence
graph nodes or verify Gemini access. Those integrations remain separate work.

A small eight-query source-known fixture lookup smoke check found the expected
match in the top five for **six of eight queries**; all returned results respected
the requested season. This is not a broad relevance benchmark. The vector-only
model sometimes ranks the reverse fixture or a different opponent ahead of the
requested home fixture. Use canonical exact filters when the entity is known;
intent/entity resolution and graph-supported retrieval remain necessary before
claiming reliable natural-language fixture analysis.
