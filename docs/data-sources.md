# Historical MVP source contracts

The MVP assumes empty storage and retains the 2025–26 scope. Collect league-wide
match evidence for comparison, with deeper player coverage for Aston Villa and
Liverpool. Public historical collection must work without paid provider accounts.

## Validated sources

| Contract | Publisher | Probe rows | Interpretation |
| --- | --- | ---: | --- |
| matches | [Football-Data.co.uk](https://www.football-data.co.uk/englandm.php) | 380 | League fixtures/results and available match statistics. Betting columns are excluded from MVP analysis. |
| teams | [FPL-Core-Insights](https://github.com/olbauday/FPL-Core-Insights) | 20 | Season-specific team identifiers and metadata. |
| players | FPL-Core-Insights | 841 | Archived player identity metadata, across the league. |
| player_totals | FPL-Core-Insights | 29,978 | Cumulative snapshots by player and gameweek. Never sum these as match-level statistics. |

These are accessibility/schema probe counts from 2026-10-03, **not a claim that
all records have been reconciled or loaded**. Appearance-level coverage will be
validated in MVP03; cross-source quality and completeness are gated in MVP04.

`config/sources.json` records each URL, season, required columns, SHA-256,
attribution, schema interpretation and reuse terms. Archive URLs pin commit
`644585b17f8db653fae9d877316b47025125e63f`. The downloadable match CSV is locked
by its content checksum. A changed hash requires review and an explicit contract
update; it must not silently change the dataset used by a demo or deduction.

The archive's [Using The Data](https://github.com/olbauday/FPL-Core-Insights/blob/644585b17f8db653fae9d877316b47025125e63f/README.md#using-the-data)
section permits project use and requests a source backlink; it does not declare
an SPDX license. Football-Data describes its data as free and supplies source
acknowledgments. Keep publisher attribution. Raw downloaded data stays local and
is excluded from the repository; source availability does not imply a blanket
redistribution license.

## Verify from an empty checkout

```bash
uv sync --locked --extra dev
uv run --locked python scripts/check_sources.py
```

The checker uses bounded downloads and network timeouts, validates checksums and
CSV schemas, checks match dates and competition, and resolves the focus-team IDs
from the archived team file. Outputs are `data/raw/source_checks/*.csv` and
`data/reports/source_checks.json`. A failed contract exits nonzero. Ingestion
retry/resume guarantees and final normalized corpus coverage are separate tickets.

Never substitute the current FPL season into this historical dataset. The legacy
live adapters require date evidence inside the configured season, and they
resolve IDs by team name. Unknown seasons have no automatic historical fallback.

## Delivery sequence

Track [MVP01–MVP10](https://github.com/jengaWiz/pl-knowledge-engine/milestone/1):
source contracts and setup → matches/players → reconciliation and resumability →
graph/text loading → traceable deductions → acceptance questions and local Docker.
Public commentary is optional enrichment; unavailable captions do not block
numerical deductions. Images, audio/video downloads, paid generation and hosted
deployment are outside the MVP gate.

## Match collection gate (MVP02)

`uv run --locked python scripts/collect_matches.py` collects from the pinned CSV,
with three bounded retries for transient network/server failures, timeouts, a
20 MB download limit, verified caching and atomic file publication. A corrupt
cache fails explicitly; `--refresh` refetches it under the same source contract.

Normalization preserves publisher team labels and row numbers while assigning
season-scoped home/away pair IDs and explicit canonical aliases. It verifies
competition, dates, scores/result agreement and shot consistency. Optional
statistics remain null; betting data is omitted. Each record carries its source
URL, hash and revision, with field-to-CSV mappings in the coverage report.

The mandatory gate requires 380 fixtures, 20 teams, 38 matches per team and
exactly one match per ordered home/away pair. Failure writes a failed coverage
report and exits nonzero before publishing normalized records. An older successful
corpus, if present, is retained; consumers must require the latest report to be
valid before loading. Multi-stage checkpoint coordination follows in MVP05.

## Player and appearance collection (MVP03)

The registry also pins 114 Premier League archive files: match metadata, lineups,
and discrete player-match statistics for each of GW1–GW38. Run `collect_matches.py`
then `uv run --locked python scripts/collect_players.py`, using the same `--output`
folder. All raw artifacts are cached and checksum-verified; the player collector
will not run against a failed match coverage report.

Join fixtures by canonical home/away teams and date. Football-Data results take
precedence for score disagreements; the original archive values and source rows
remain in `fixture_conflicts`. Persistent team codes are distinct from seasonal
FPL team IDs. Player clubs come from match lineups, preserving transfer attribution.
Appearances are keyed by player and fixture, so double gameweeks remain distinct.
`minutes_played` is authoritative; it is never inferred from substitution times.

Player totals select the latest available FPL snapshot instead of summing repeated
cumulative values. They are explicitly labeled player-season totals across clubs.
Per-match FPL points and FPL clean sheets are unavailable in this archive and stay
null. Archive assists and FPL awarded assists are different metrics.

The focus-team gate requires 38 reconciled fixtures per team, known player
identities, match-team attribution and statistics for recorded starters. Missing
bench statistics remain disclosed and unknown; they are never synthesized as
zero-minute appearances. Gaps for non-focus clubs are reported separately.

The initial collection retained 57 players and 1,284 player-match records,
including 1,162 positive-minute appearances. All focus fixtures and recorded
starters have evidence. It disclosed one score conflict and 36 unmatched appearance
keys in non-focus fixtures. Cross-source completeness and per-team metric
reconciliation are checked further in MVP04; these figures are coverage of the
available corpus, not a claim that every possible source metric is complete.

## Reconciled corpus gate (MVP04)

Run `uv run --locked python scripts/check_corpus.py` after both collectors. Collection
reports record hashes of normalized artifacts. The quality gate verifies them,
season and identifier uniqueness, publisher contracts, player/fixture joins,
match-team/date attribution, and recorded player goals against primary team scores.
It produces `quality.json` and `quality.md`. Missing evidence or failed collection
reports cannot pass; a loader must use `load_verified_corpus` to reject artifacts
changed after validation.

The initial real corpus passed with 380 matches, 57 players and 1,284 appearance
records. Seven warnings disclose five fixtures with goals not attributed to player
records, the source score conflict, and substitute/bench entries without statistics.
Unattributed goals may reflect own goals or source omissions; no player totals are
invented to force agreement. Archive FPL season metrics remain separate from
match-stat provider metrics and club-specific appearance totals.

## Repeatable collection (MVP05)

`run_mvp.py` is the default no-key collection workflow. It serializes one writer per
data directory, writes running/failed/completed stage manifests atomically, and
stops before downstream stages on a mandatory failure. Restarting validates and
reuses completed download caches, then deterministically rebuilds and checks the
small derived artifacts. It never skips validation based only on a completed flag.
Changed source hashes require explicit contract review; corrupt caches can be
refetched with `--refresh`. Normalized IDs and bytes remain stable on a cache rerun.

Shared legacy checkpoints are now atomic and restore their in-memory state if a
write fails. Embedding output is flushed and fsynced before marking its checkpoint;
this durability change does not invoke a provider. The legacy pipeline also stops
when a stage raises. Stage completion and provider-specific data completeness are
separate; the new MVP quality gates define the accepted corpus.

## Verified graph and local retrieval (MVP06)

After collection and the quality gate, load the stores with:

```bash
# Set NEO4J_URI, NEO4J_USER and NEO4J_PASSWORD for your local database.
uv run --locked python scripts/load_mvp.py --output data
```

The loader creates season-scoped, provenance-bearing nodes for all 20 clubs,
380 fixtures, 38 gameweeks, 57 focus-club players and 1,284 appearance records.
It checks node/relationship counts and rejects dangling appearances inside a
single write transaction. Repeated loads use stable identities; cleanup only
covers nodes and relationships marked as managed by this MVP for this season.
Unrelated graph data is preserved. Use a dedicated Neo4j database for the demo.

Chroma stores 457 source-linked summaries: 380 fixtures, 20 teams and 57 players.
Team points are calculated from results, excluding administrative deductions.
Player summaries explicitly distinguish archive assists from FPL assists and
cover the retained focus-club appearances. Retrieval filters include both clubs
when a player represented more than one team.

The pinned local `all-MiniLM-L6-v2` ONNX model produces 384-dimensional vectors
for both documents and queries. Its archive checksum is enforced by Chroma's
model downloader and the application contract. The first run downloads the
public model; inference then runs on the CPU without provider credentials.
This index uses a separate collection from legacy Gemini embeddings. Artifact
checksums and the summary schema version determine its collection namespace.

`data/reports/mvp/2025-26/stores.json` exposes dataset/model versions and counts;
readiness is published only after both stores load successfully. Failed loads
replace the previous readiness report with an explicit error. Changed or failed
quality evidence blocks loading; stale index manifests block retrieval.

Acceptance verification used the complete collected corpus and local Neo4j
5.26 Community: two consecutive loads retained identical counts, preserved an
unmanaged sentinel node, and semantic search returned the Liverpool team
summary with its primary CSV source. Automated tests also exercise persistent
Chroma reloads, transferred-player filters, invalid vectors, model-contract
changes, stale evidence and failed readiness publication. The current API chat
still uses the legacy path; wiring these verified stores into evidence-backed
answers is tracked in MVP08.

## Optional public metadata and fallback (MVP07)

```bash
uv run --locked python scripts/collect_commentary.py --output data
# Or include this optional stage after the core quality gate:
uv run --locked python scripts/run_mvp.py --output data --commentary
# Reload stores after collecting or changing commentary:
uv run --locked python scripts/load_mvp.py --output data
```

Discovery starts with the official Aston Villa and Liverpool news pages and
follows at most two advertised RSS/Atom feed links per source. A curated JSON
manifest supports official-club article URLs and YouTube episode/feed URLs
without an API key:

```json
[
  {
    "url": "https://www.liverpoolfc.com/news",
    "team": "Liverpool",
    "publisher": "Liverpool FC"
  }
]
```

Use `--manifest path/to/sources.json`. Each entry requires `url`, `team` and
`publisher`; an explicitly curated article may also supply `title` and
`published_at` from its publication metadata. Only HTTPS URLs on the official
club or YouTube hosts are accepted. Robots policy is checked before access;
redirects and authentication are excluded. Requests have timeouts and a 2 MB
content limit. Collection is bounded to 20 manifest sources, three requests per
source, and ten retained items per focus team.

The stored corpus contains publication titles, dates, URLs, publishers, club
attribution and response checksums. Whole articles, captions and audio/video
are excluded from this MVP collector. YouTube Atom feeds can contribute episode
metadata; unavailable captions do not block statistics-based answers. An empty
caption timestamp list and the report's caption status make this limit explicit.
Titles and publisher opinion are labeled separately from measured statistics.

The real discovery run reached both official news pages but found **zero usable
dated 2025–26 items for each club** in their advertised metadata. This falls
short of the ten-item enrichment target. The report therefore marks external
commentary unavailable and retains all 457 verified match/team/player summaries
as the MVP fallback. It does not invent quotations or infer that an episode
contains a particular opinion from its title.

`data/reports/mvp/2025-26/commentary.json` records each attempt and coverage;
`data/cleaned/mvp/2025-26/commentary.jsonl` stores the accepted metadata. Its
verified checksum participates in the index version, so adding, removing or
changing commentary requires reloading stores. Missing or altered artifacts
fail verification. Tests cover RSS/Atom and JSON-LD parsing, season filtering,
deduplication, denied access, bounds, publisher provenance and stale indexes.
