# MVP acceptance evidence

Validated on 4 October 2026 (America/Los_Angeles), against pinned 2025–26 sources.
CI separately runs offline Python 3.11/3.12 tests, lint, packaging and frontend builds.

| Check | Result |
| --- | --- |
| Collection into empty storage | 380 matches, 20 clubs, 57 players, 1,284 appearances |
| Cold-run normalized checksums | Exactly reproduce the original verified corpus |
| Independent numerical oracle | 42 questions from raw CSVs using SQLite; all passed |
| Insufficient-evidence cases | Five passed: wrong season, player comparison, missing xG, commentary and excessive minutes threshold |
| Live numerical API | All 42 reference questions passed |
| Real graph | 1,780 nodes; counts and versions match the verified corpus |
| Graph API | Player surname lookup, fixtures, match navigation and edge integrity passed |
| Ambiguous names | Return 409; temporary acceptance nodes are cleaned up |
| Local semantic search | 457 summaries; filtered team retrieval passed |
| Production browser suite | 8/8 desktop/mobile checks passed without retries or skips |
| Frontend dependency audit | Zero reported vulnerabilities after compatible upgrades |
| Docker cold setup | Pinned non-root image, empty named volumes, public collection and local CPU model; readiness passed |
| Docker live store/API acceptance | Seven checks passed, including all 42 live numerical references |
| Docker browser suite | 8/8 desktop/mobile checks passed against the served production dashboard |
| Consistent private backup | All four volumes archived while project services stopped; archive and password file mode 0600 |
| Restart persistence | No recollection; graph/index versions, corpus counts and all live numerical references preserved |

[Committed reference questions](../tests/fixtures/mvp_reference_questions.json)
cover team points, goals, results, goal difference, home/away, last-five form and
player goal/assist/per-90 leaders. The [generator](../scripts/build_reference_questions.py)
uses raw CSVs and SQLite and imports no normalizer, deduction engine or API code.
Archive lineups attribute player metrics to the club in each fixture. The golden
file records source checksums and contributing primary CSV rows. Updating it
requires reviewing source contracts and independently recalculating expectations.

The [checker](../scripts/accept_mvp.py) requires exact numerical/rounding agreement
and traceability. Wrong values, invented records and wrong source URLs fail the
gate. Existing source-quality warnings remain visible in reports.

## Reproduce

```bash
uv sync --locked --extra dev
uv run --locked python scripts/run_mvp.py --output /tmp/pl-fresh-data
uv run --locked python scripts/accept_mvp.py --output /tmp/pl-fresh-data
```

Configure local Neo4j credentials, load stores and start the API with DATA_DIR
pointing to the populated data folder. Then run:

```bash
uv run --locked python scripts/accept_stores.py --output data --url http://127.0.0.1:8011
cd frontend
npm ci --ignore-scripts
npx playwright install chromium
npm run build
PL_API_TARGET=http://127.0.0.1:8011 npm run preview -- --host 127.0.0.1 --port 5174
# Another terminal:
PL_DEMO_URL=http://127.0.0.1:5174 npm run test:e2e
```

Browser checks cover actual graph/player/match navigation, keyboard selection,
UTC fixture dates, deductions, source opening, mobile layout and recovery from
simulated API outages. Publisher popup responses are controlled locally, while
source URLs are verified by corpus/source contracts. Production builds avoid
reloads during acceptance.

Reports live at data/reports/mvp/2025-26/acceptance.json and store_acceptance.json;
browser results and failure traces live in frontend/test-results. Collected data,
model files, credentials and these generated artifacts remain outside Git.

Graph reads use bounded timeouts and only the managed configured season.
Unknown and ambiguous names return explicit errors. Docker cold setup, browser
flows, backup and restart persistence were verified on this machine. The
generated deployment.json records post-restart readiness and store acceptance.
See [local deployment](local-demo.md) for reproduction. Public hosting remains
outside this no-cost MVP.
