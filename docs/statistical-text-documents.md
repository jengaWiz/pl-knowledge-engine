# Source-backed statistical text documents

This optional preparation stage turns the verified statistical MVP into useful
retrieval text without embedding or provider calls. It creates one description
per completed match and one per positive-minute player appearance. Zero-minute
records and repeated cumulative FPL snapshots do not inflate document counts.

## Run

Collect and validate the MVP first (`make mvp`), then:

```bash
uv run --locked python scripts/prepare_text.py
# For an existing corpus in a different directory:
uv run --locked python scripts/prepare_text.py --output /path/to/data --season 2025-26
```

Defaults cap preparation at 5,000 documents and 10,000,000 bytes of UTF-8 text;
`--document-budget` and `--byte-budget` change these limits. Manifest metadata and
original CSV downloads are separate from the text payload byte budget. The stage
uses the MVP pipeline lock and makes no network requests.

## Evidence and identities

Before rendering, the loader checks the accepted normalized corpus checksum and
revalidates the pinned checksum/schema of every contributing original CSV file.
It checks match statistics against normalized raw match rows, and appearance
metrics, player identities, lineup team codes, archive fixture dates and canonical
home/away teams against their original rows. Unknown metrics remain `unknown`.
Primary Football-Data results take precedence over conflicting archive scores,
as in the MVP. Existing completeness warnings still apply.

Each description is a derived seasonal text asset with its actual fixture date.
Its document range is `0..len(text)` in Unicode characters; byte lengths and
checksums use UTF-8. Canonical player and match IDs connect it to the existing
graph. Appearance documents also retain their appearance record ID separately.

Each document carries all contributing row references: original source ID,
content-addressed asset ID, URL, checksum, revision and row number. Appearance
references include statistics, lineup, player identity, primary result, team-code
mapping and archived fixture. A derived asset's single primary parent links to
its main statistics file; `source_refs` retains the other contributing sources.

Original CSV files count once as source assets; descriptions count as derived
assets and retrieval documents. Whole CSVs may contain many dates or no explicit
event date, so their asset contracts use undated background scope. The source
contract in the extraction profile records their season; derived descriptions
carry the actual seasonal evidence date. The undated raw file does not itself
establish a single-event date.

Publisher reuse terms and reference links are preserved verbatim in asset
metadata. The archive does not declare an SPDX license; this stage does not
invent one or publish raw data. Original and generated data remain ignored local
artifacts.

## Acceptance and outputs

- `cleaned/multimodal/<season>/text_assets.jsonl`: original and derived assets.
- `cleaned/multimodal/<season>/text_documents.jsonl`: documents, text and references.
- `reports/multimodal/<season>/text.json`: status, profile, hashes and honest counts.

Reports are invalid while work runs. Failure withdraws both accepted manifests;
reruns deterministically regenerate them from verified inputs. The extraction
profile includes schema, season, corpus checksum and contributing source
contracts. `load_text_documents(output, season)` rechecks the original files and
reproduces the manifests before allowing downstream use. Altered text, row
references, profiles, counts or source bytes fail the gate.

The verified 2025–26 local corpus produced **1,542 unique text documents** from
**117 original CSV sources**: 380 matches and 1,162 positive-minute appearances.
These prepared documents are not yet embedded or exposed through the demo index.
The default index remains the existing 457 statistical summaries. This stage
alone does not meet the 2,500-document multimodal target.
