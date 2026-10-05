# Run the local Docker demo

This portfolio deployment deliberately runs on your machine. It avoids ongoing
hosting/database bills and uses free public data with local CPU inference.
The dashboard and API share a multi-stage, digest-pinned image, run as a non-root
user, and bind only to loopback. This is a reproducible local deployment; public
hosting, authentication and production hardening remain separate future work.

## One command from empty storage

Install Docker Desktop (or Docker Engine with Compose), start the engine, and
have Python 3 available for the standard-library launcher. Then, from the root:

```bash
python3 scripts/demo.py
```

The launcher creates a private database password, starts Neo4j and the API,
collects pinned historical sources, validates coverage/provenance, loads both
stores, runs the numerical acceptance gate, and checks actual readiness.
Open **http://127.0.0.1:8010**; API docs are at **/docs**.
No cloud account, billing setup, Gemini key or YouTube key is needed.

The initial run needs internet access for container images, Python/npm packages,
118 source contracts and the approximately 80 MB embedding-model archive. Allow
several minutes, at least 4 GB of Docker memory and several GB of available disk.
Subsequent runs reuse verified source caches and the local model. Inference runs
on CPU. No media download or paid provider stage runs in this workflow.

| Persistent volume | Contents |
| --- | --- |
| `pl-knowledge-engine-demo_corpus` | Raw/normalized data, checkpoints and quality/acceptance reports |
| `pl-knowledge-engine-demo_graph` | Neo4j database |
| `pl-knowledge-engine-demo_index` | Chroma vector index |
| `pl-knowledge-engine-demo_models` | Local ONNX embedding-model cache |

Private credentials live in `data/private/demo.env` with mode 0600. Keep that
file with the corresponding graph volume; changing the password file alone does
not change an initialized database's password. Generated data and secrets are
excluded from both Git and the image build context.

## Coverage and provenance

The fixed season is **2025–26**, with primary match dates **15 August 2025 through
24 May 2026**: 380 fixtures for all 20 clubs. Detailed player data covers Aston
Villa and Liverpool: 57 players, 1,284 appearance records, and 1,162 appearances
with positive minutes. The graph has 1,780 nodes and local retrieval has 457
source-linked summaries. Missing bench statistics stay missing; cumulative FPL
snapshots are kept separate from match statistics.

[Source contracts and attribution](data-sources.md) identify Football-Data and
FPL-Core-Insights, checksums, archive revision and permitted usage. Sources are
fetched on the user's machine, rather than redistributed as a bundled dataset.
Quality reports disclose one conflicting score, five unmatched team goals and
missing bench statistics. Primary match results determine team deductions.
Optional publisher discovery currently provides no usable season-dated
commentary, so the demo uses verified statistical evidence and explicitly
abstains from unsupported causal, predictive or commentary claims.

## Readiness, inspection and recovery

```bash
python3 scripts/demo.py status
python3 scripts/demo.py stop
python3 scripts/demo.py             # resume without deleting volumes
python3 scripts/demo.py --recollect # verify caches, rebuild and recheck stores
```

`GET /api/health` means the API process responds. `GET /api/readiness` returns
200 only when the verified corpus, current graph, compatible vector collection
and cached model are present; it returns 503 otherwise. Collection or validation
failure stops setup and preserves reports for diagnosis. A running container
alone never proves that the application has usable data.

To inspect only this project's logs:

```bash
docker compose --env-file data/private/demo.env logs --tail 100 app neo4j
```

If Docker Desktop hangs before any build step, check its credential helper.
The build can use a separate `DOCKER_CONFIG` containing the existing context and
plugin paths without changing the user's login. This machine-specific workaround
was needed during local verification; it is not a repository requirement.

Ports are loopback-only: dashboard/API **8010**, Neo4j Bolt **7688**, Neo4j Browser
**7475**. A conflicting listener must be stopped or these project ports changed.
The launcher manages only `pl-knowledge-engine-demo`; it does not stop other apps.

## Offline backup and explicit reset

```bash
python3 scripts/demo.py backup
```

Backup stops only this project's containers for a consistent snapshot, archives
all four volumes into `data/backups/<UTC timestamp>/volumes.tar.gz`, copies the
matching private password, and restarts the project. The directory is private,
and both files have mode 0600. Store them securely together and outside Git.

To restore a backup, stop this project, restore each `volumes/<volume-name>`
archive directory to its corresponding named volume while the services are
stopped, and restore the matching `demo.env` before starting. Never restore a
live Neo4j or Chroma directory. Use an isolated project for recovery exercises.

**Reset deletes this demo's collected data, graph, index and model cache.** Only
run it when you intend to discard those four volumes:

```bash
docker compose --env-file data/private/demo.env down --volumes
python3 scripts/demo.py
```

The private password file can be reused after a full reset. Do not use system-wide
Docker prune commands; other projects and their volumes are unrelated.

## Acceptance checks

After setup, the populated Docker application can run the same live store/API
checks and browser suite used for review:

```bash
docker compose --env-file data/private/demo.env exec -T app \
  python scripts/accept_stores.py --url http://127.0.0.1:8000 --output /app/data
cd frontend
npm ci --ignore-scripts
npx playwright install chromium
PL_DEMO_URL=http://127.0.0.1:8010 npm run test:e2e
```

See [acceptance evidence](mvp-acceptance.md) for numerical reference methodology,
coverage, browser behavior and recorded restart checks.
