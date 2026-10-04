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
