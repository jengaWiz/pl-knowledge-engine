# Graph-constrained local evidence retrieval

The accepted 2,682-document corpus now supports a narrow natural-language
retrieval pipeline: deterministic canonical routing → bounded Neo4j candidates
→ Chroma ranking within those candidates → exact document/provenance join.
It uses the existing cached 384-dimensional all-MiniLM-L6-v2 ONNX model and local
stores. No Gemini calls or paid services are involved.

## Use

Prepare the structured corpus, evidence index and evidence graph first, then:

```bash
uv run --locked python scripts/search_evidence.py \
  'Liverpool home match against Bournemouth' --evidence-season 2022-23
uv run --locked python scripts/search_evidence.py \
  'Digne appearances against Liverpool' --evidence-season 2025-26
uv run --locked python scripts/accept_retrieval.py
```

Configure Neo4j credentials through the environment as in the local operations
guide. Use `--output /path/to/data` and `--season` for the primary corpus directory
and season. `--evidence-season` is always explicit; it selects the historical
season to search and is not inferred from the current date.

Responses contain the canonical route, graph dataset hash and ranked evidence
with document IDs, local cosine distance, source metadata and graph records.
Fixture records include exact home/away clubs and scores. Player records include
the player, appearance, match, actual team and opponent. These are retrieved
records, not generated analysis, calibrated confidence scores or predictions.

## Supported requests

| Request | Behavior |
| --- | --- |
| `Liverpool home match against Bournemouth` | Resolves the home fixture. |
| `Liverpool away match against Bournemouth` | Resolves the reverse fixture. |
| `Liverpool at Bournemouth` | Treats Liverpool as the visitor. |
| `Liverpool vs Bournemouth on 2025-08-15` | Resolves an exact ISO fixture date. |
| `Digne appearances against Liverpool` | Traverses player → appearance → match/opponent. |
| `Lucas Digne appearances` | Retrieves the player's bounded appearance set. |
| `Liverpool vs Bournemouth` | Requests home/away or date clarification. |
| A query season conflicting with the selection | Requests clarification. |
| Prediction, causal or tactical requests | Returns unsupported with no retrieval. |

Full player names and unique surnames resolve within the selected season.
Ambiguous surnames request clarification. Club aliases such as Villa, Man City,
Man Utd, Spurs and Wolves map to the source's canonical club names. This is a
conservative phrase-based router, not a general language model: unrecognized
names, multi-player comparisons and player home/away/date constraints need
clarification or a future dedicated route. It does not approximate unknown names.

## Scope and integrity

Graph reads verify accepted sources and exact stored graph properties first.
Fixture candidate sets contain one exact match document. Player sets contain at
most the **50 most recent matching appearances**, with an optional exact opponent.
Chroma receives season, kind, player and a canonical match allowlist before
ranking; no unrelated opponent or reverse fixture can enter through vector
similarity. Results are bounded to 1–50 and query text to 4,000 characters.

The vector index independently verifies its accepted corpus, model profile,
stored text/provenance and checkpointed vector hashes. Joined hits must agree on
document identity, text, season, match, player and every source reference. Missing
or duplicate documents and mismatched provenance fail closed. Accepted evidence
is reconstructed again after retrieval to detect source changes between stores.
There is no distributed snapshot transaction across Neo4j, Chroma and files.
Concurrent external changes can require retrying; full verification adds latency
and has not been optimized for production request volume.

## Acceptance and remaining integration

The frozen smoke set is `tests/fixtures/evidence_retrieval_questions.json`. Its
eight fixture questions are unchanged from the earlier vector-only smoke run;
expected match identities and scores are stored independently of routing.
Two player/opponent cases require both corresponding appearances, and six
negative cases require the expected abstention status with zero hits. The CLI
writes an ignored `reports/evidence/<primary-season>/hybrid-smoke.json` report,
including fixture checksum and individual results. Failures withdraw validity.
The verified local run passed **16/16 cases**: eight fixtures, two player/opponent
checks and six abstentions. The same eight fixture questions previously found the
expected match in the vector-only top five in 6/8 cases; this pipeline returned
the exact fixture and scores at rank one in 8/8.

This is a small source-known smoke set, not a broad semantic relevance benchmark.
Graph constraints address the home/away and opponent ambiguity in these supported
routes; they do not establish arbitrary natural-language understanding.

The default analyst API and React interface still use the established MVP.
Connecting this evidence retriever to those endpoints, broader independent
retrieval evaluation, temporal prediction evaluation and Gemini free-tier
verification remain pending. The existing 457-summary MVP index remains intact.
