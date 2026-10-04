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
