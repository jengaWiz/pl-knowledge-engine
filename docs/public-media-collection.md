# Reviewed public media collection

The optional Commons collector adds actual image, audio and video assets without
paid APIs. It does not alter the default statistical MVP or claim that a complete
multimodal corpus is indexed.

## Run

Install FFmpeg (including ffprobe) and the locked Python dependencies, then run:

```bash
uv run --locked python scripts/collect_media.py
# Or isolate the collection:
uv run --locked python scripts/collect_media.py --output /path/to/media-data
```

The command uses three reviewed pins in `config/commons_media.json`:

| Source | Terms | Role |
| --- | --- | --- |
| [Girls football training](https://commons.wikimedia.org/wiki/File:Girls_football_training.webm) | Evworo, CC BY-SA 3.0 | General football background video, 2019 |
| [Anfield Road Stand in May 2024](https://commons.wikimedia.org/wiki/File:Anfield_Road_Stand_in_May_2024.jpg) | FYI2023, CC0 | Stadium background image, 2024 |
| [Premier League spoken article](https://commons.wikimedia.org/wiki/File:Premier_League.ogg) | Hassocks5489 / article authors, CC BY-SA 3.0 | Historical audio background, 2007 |

All three are explicitly **background** evidence. They do not establish current
player performance, 2025–26 club tactics or prediction features. They are sampled
source assets, not 2,500 documents. Further sources require review and attribution;
this collector does not scrape arbitrary URLs or automatically authorize new
media just because it is publicly reachable.

## Verification and recovery

For each pin, the collector queries the Commons REST-style MediaWiki API and
compares returned attribution, license URL/name, event date and byte size with
the reviewed record. It only downloads from the Wikimedia upload host over
HTTPS and disallows redirects. A changed checksum or terms fails rather than
silently replacing the source contract.

Downloads are bounded to 20 MB per asset, 50 assets per run and a default total
budget of 50,000,000 bytes. `--byte-budget` explicitly changes the total budget.
Transient network failures are retried at most three times. A file becomes cached
only after a complete, checksum-verified download and atomic write.

ffprobe verifies real streams and ffmpeg decodes up to two seconds. This is a
bounded decoding smoke check, **not a claim that every frame or second has been
validated**. Full extraction and segmentation remain separate stages.

Reruns recheck source metadata and cached byte identity, and decode the sample
again. They reuse valid downloads. A corrupt cache is reported as a failure and
is not silently replaced; remove only that reported cached file if you intend to
refetch the same reviewed source. No global prune or unrelated data reset is
needed.

The collector locks its output folder. The run report is invalid while work is
in progress; failed runs invalidate the accepted manifest. Successfully downloaded
files remain cached, so a later run can resume without inflating source counts.

## Outputs and downstream gate

- `raw/multimodal/commons/<sha256>.blob`: verified media bytes.
- `raw/multimodal/metadata/<sha256>.json`: original source/license metadata snapshot.
- `cleaned/multimodal/commons_assets.jsonl`: accepted asset contracts, only after all sources pass.
- `reports/multimodal/commons.json`: status, counts, cache hits, decoding results,
  metadata checksums and source-specific failures.

`load_verified_media(output)` requires a valid report, a matching manifest hash,
current pinned provenance and unchanged media/metadata bytes before downstream
processing. It fails on tampering or incomplete collection. Counts report original
assets separately; this stage creates **zero retrieval documents or embeddings**.

Media, snapshots and reports stay outside Git and the Docker image context.
The default demo image does not include FFmpeg; this optional collector currently
runs in native development. No Gemini or YouTube calls occur.

## Prepare retrieval documents

After collection succeeds, run:

```bash
uv run --locked python scripts/extract_media.py
# Use the same --output directory as the collector, if customized.
```

This stage creates whole-image documents and consecutive 20-second audio/video
segments, including the final shorter segment. Audio becomes mono 16 kHz PCM WAV.
Video becomes H.264 at two frames per second, at most 480 pixels wide, without
audio. These are retrieval payloads, not transcripts or embeddings. The original
files and attribution remain available through each document's parent asset ID.
Video coverage comes from visual packet timestamps rather than the container's
audio tail. Output duration is checked within encoding/frame-rate tolerance, and
every produced audio/video segment is fully decoded before acceptance.

Defaults cap output at 500 documents and 100,000,000 payload bytes. Use
`--segment-seconds` (5–60), `--document-budget` or `--byte-budget` to change these
limits. A profile hash includes segment length, FFmpeg version and source
contracts. Successful chunks are checkpointed individually; interrupted runs
resume verified chunks from that profile. Changed or corrupt payloads fail
validation. A different profile creates a new extraction; previous content is
retained, so disk usage can exceed a single run's payload budget.

Accepted outputs are `cleaned/multimodal/media_documents.jsonl`, content-addressed
payloads under `cleaned/multimodal/content/`, and the report
`reports/multimodal/extraction.json`. `load_media_documents(output)` checks source
provenance, profile, payload hashes, ranges and counts before downstream use.
The manifest is withdrawn during a run and published only when the entire
extraction passes. Per-chunk checkpoints stay under `checkpoints/media/`.

The source remains historical/background evidence after segmentation. More
segments do not mean more original sources or current tactical coverage. This
stage makes no network requests, creates no vectors and does not change the
default demo index.
