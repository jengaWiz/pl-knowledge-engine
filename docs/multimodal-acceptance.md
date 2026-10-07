# Optional multimodal corpus acceptance

The default [structured evidence route](structured-evidence-acceptance.md) makes
media optional. This document describes `--mode multimodal`; schema v2 requires
rerunning acceptance for this mode if a v1 report exists.

This stage combines source-verified statistical text with extracted public media,
retaining their asset contracts, document identities and contributing references.
It prepares retrieval artifacts; it does not create embeddings or change the
API's default demo index.

## Run

Prepare each input stage first, using the same output directory:

```bash
make mvp
make collect-media
make extract-media
make prepare-text
uv run --locked python scripts/accept_corpus.py --mode multimodal
# A milestone gate that exits unsuccessfully until both target conditions hold:
uv run --locked python scripts/accept_corpus.py --mode multimodal --require-target
```

The media stages require native FFmpeg/ffprobe. For custom data directories, pass
`--output /path/to/data` to each corresponding script. The default demo image
does not include FFmpeg. All these stages use no Gemini or YouTube calls.

Acceptance first snapshots the input manifests/reports, invokes both downstream
source/payload gates, then checks that the snapshot stayed unchanged. If inputs
move during verification, acceptance fails and can be retried. Combined output
has its own lock; source stages keep their existing locks. Consumers must use
`load_corpus(output, season, mode="multimodal")` to revalidate inputs and reproduce the combined
manifests before use, rather than trusting a stale report alone.

Default limits are 5,000 traceable document records and 150,000,000 unique payload
bytes. `--document-budget` and `--byte-budget` change them. Manifest metadata and
original files are separate from the payload budget. No media payloads are
copied: the manifest refers to their existing content-addressed files. Text
records retain inline text. Failure withdraws both combined manifests without
disturbing accepted source-stage artifacts.

## Count what is actually prepared

The current verified local 2025–26 corpus contains:

| Measure | Verified result |
| --- | ---: |
| Original source assets | 120: 117 CSV files and three media files |
| Derived text assets | 1,542 |
| Unique retrieval documents | 1,648 |
| Text documents | 1,542: 380 matches and 1,162 positive-minute appearances |
| Audio documents | 104 |
| Image documents | 1 |
| Video documents | 1 |
| Unique payload bytes | 67,366,221 |
| Seasonal document records | 1,542 |
| Historical/background document records | 106 |
| Embeddings created by this stage | 0 |

Original CSV file contracts are undated; their derived descriptions carry actual
fixture dates. Media remains historical/general football background. Four
modalities do not imply current-season tactical footage, transcripts or
multimodal prediction capability.

`count_corpus` deduplicates document content by modality and checksum, while
retaining distinct parent/extraction records for attribution. Identical content
does not inflate the target or payload-byte totals. Shared asset identities with
conflicting provenance are rejected rather than silently overwritten.

The current status is **`prepared_below_target`**, with **852 additional unique
documents needed**. The target requires at least 2,500 unique documents and
nonzero document counts in all four modalities. `--require-target` returns a
nonzero exit code when either condition fails, while leaving a valid report of
the actual prepared corpus. A valid corpus report alone is not a completed
milestone or a live index acceptance result.

## Outputs

- `cleaned/multimodal/<season>/corpus_assets.jsonl`: combined source and derived assets.
- `cleaned/multimodal/<season>/corpus_documents.jsonl`: combined records with stage labels.
- `reports/multimodal/<season>/corpus.json`: source-stage hashes, manifest hashes,
  deduplicated counts, record scopes, seasonal date range and target status.

The report is invalid during acceptance. Consumers recheck source files, media
payloads, text lineage, stage snapshots, combined manifests and report counts.
Generated data, source downloads and private implementation planning remain
outside Git. Gemini model integration, vector indexing and larger source coverage
are separate remaining work.
