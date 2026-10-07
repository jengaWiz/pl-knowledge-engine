# Local evidence graph

The optional structured corpus now has an independently owned Neo4j projection.
It connects verified documents and original CSV sources to season-specific matches,
teams, players and positive-minute appearances. The default analyst API and React
interface still use the established MVP; combining this graph with the evidence
vector index and natural-language entity routing remains separate work.

## Load

Prepare and accept the structured corpus, start local Neo4j, then run:

```bash
uv run --locked python scripts/accept_corpus.py --require-target
uv run --locked python scripts/load_evidence_graph.py
```

Configure `NEO4J_URI`, `NEO4J_USER` and `NEO4J_PASSWORD` as described in the local
operations guide. Pass `--output /path/to/data` and `--season` for another data
location or primary season. Loading uses the existing local database and no paid
API. Its ignored readiness report is `reports/evidence/<primary-season>/graph.json`.

The verified local projection contains:

| Component | Count |
| --- | ---: |
| Accepted document nodes | 2,682 |
| Original CSV source nodes | 120 |
| Match entities across four seasons | 1,520 |
| Season-specific team entities | 80 |
| Referenced players in 2025–26 | 53 |
| Positive-minute player appearances | 1,162 |
| Season nodes / corpus root | 4 / 1 |
| Total owned evidence nodes | 5,622 |
| Internal evidence relationships | 31,142 |
| References to existing MVP entities | 1,595 |

Four of the MVP's 57 roster players have no accepted positive-minute appearance
here, so the evidence projection has 53 player entities. It preserves unknown
statistics as absent properties instead of assigning zero. Source relationships
retain all cited row/revision references; derived text asset and document
contracts remain on document nodes.

## Bounded reads

```python
from pathlib import Path
from src.store.evidence_queries import read

hits = read(
    Path("data"), "2025-26", "bolt://localhost:7687", "neo4j", password,
    evidence_season="2022-23",
    entity_id="pl:2022-23:liverpool:bournemouth", kind="fixture", limit=1,
)
```

Fixture reads return exact home/away identities, scores, document text and source
references. Player reads use `kind="player"`, a canonical player ID and an optional
exact opponent name; they follow player → appearance → match/team relationships
and return source-linked appearance evidence. Path reads use `kind="path"` plus
`end_id` and `max_hops` (1–4) to return one shortest association path. These paths
show recorded connections; they are not evidence of causation or tactical effect.

Every read requires an explicit season, validates canonical entity types, and
limits results to 1–50. Only a validated integer path bound enters the Cypher
text; user identities and opponent values are parameters. Managed read
transactions have a ten-second timeout. Reads reconstruct accepted source data
and check the entire projection and canonical references before querying. This
prototype favors integrity; those full checks add latency and should be measured
before optimizing or exposing this interface to the UI.

## Isolation and verification

Owned nodes carry `EvidenceNode` plus a typed `Evidence*` label, a unique evidence
key, dataset hash and owner namespace. A single transaction writes the projection,
prunes stale owned data and verifies exact node labels/properties and relationship
properties before commit. Canonical references point to existing `mvp_managed`
entities without rewriting those nodes. Failed builds withdraw readiness; stale
source data, graph mutations and changed references fail closed.

The live repeat load passed. The original **1,780 MVP nodes and 3,824 internal
relationships** had identical property hashes before and after. Eight canonical
fixture reads across four seasons returned the expected scores; a player-opponent
read returned both Digne appearances against Liverpool, and a three-hop path
connected him to that club. A deliberately invalid transactional document write
was rejected and rolled back.

These exact canonical checks do not resolve natural-language ambiguity: the
separate vector smoke result remains 6/8 expected fixtures in the top five.
Hybrid routing, analyst integration, prediction evaluation and live Gemini
verification remain pending. No provider calls or cloud spending were needed.
