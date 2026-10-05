# Multimodal corpus contracts

The verified statistical MVP remains the default deployed corpus. The contracts
in `src/corpus/contracts.py` establish validation for additional media; they do
not claim that a multimodal corpus has already been collected or indexed.

## Assets and retrieval documents

An **asset** is one downloaded original or an explicitly declared derivative.
Its identifier is the SHA-256 of its bytes. Required provenance includes the
original source and reference URLs, publisher, author attribution, license name
and license URL. MIME type must agree with the declared modality. These fields
record source evidence; storing a license string does not independently prove
permission or validate that the source genuinely supplied those terms.

An extracted audio track retains its video's `parent_asset_id`. Original source
counts exclude such derivatives. Missing parents, cyclic derivation and duplicate
asset IDs fail validation.

A **retrieval document** is an extracted unit with its asset ID, modality, content
checksum, extraction version and location. Text uses integer character offsets;
audio/video uses finite second ranges; a whole image uses the sentinel range
0..1. A document ID includes its parent, range, extraction version and checksum.
The manifest links documents to matching-modality parents. Range/offset validation
does not replace actual decoding or extraction against the source bytes.

## Scope

Season-specific evidence requires a validated season and an event date within its
bounds. Undated or historical background material cannot be labeled as coverage
of the current season. Unknown publication dates stay null; background assets may
carry known historical event dates without implying a season.

Collectors must verify entity references against the canonical graph and retain
unresolved mentions separately. The contract stores entity IDs without inventing
club, player or fixture assignments. Provider-specific MIME conversion and limits
belong to embedding adapters; valid corpus media are not automatically valid API
inputs.

## Counting

`count_corpus` returns:

- Original source assets and derived assets separately.
- Unique retrieval documents, deduplicated by modality and content checksum.
- The number of duplicate-content document records.
- Source-asset and retrieval-document totals per modality.
- A target flag that requires at least 2,500 unique documents and nonzero text,
  image, audio and video coverage.

An embedding upsert, repeated rerun, alternate URL or duplicate chunk must not
inflate coverage. The count report does not claim vectors were embedded or
indexed; later store acceptance must reconcile those counts independently.
“2,500 source documents” requires 2,500 independent source assets. A corpus with
fewer sources and 2,500 derived chunks must describe them as retrieval documents
or chunks and disclose the separate source count.
