# Historical outcome baseline

This offline evaluation estimates home/draw/away probabilities from prior match
results. It underpins the [historical prediction API and UI](prediction-workflow.md); the
ordinary chat and Source evidence endpoints still abstain from predictions.
No Gemini calls, API keys, graph database or embeddings are needed for evaluation.

## Reproduce

Use Python 3.11 or 3.12 with the project's locked environment. If historical sources
are not cached, collect the three reviewed seasons first:

```bash
uv run --locked python scripts/collect_history.py
uv run --locked python scripts/evaluate_prediction.py
```

`--output PATH` selects the data directory for either command. Evaluation performs
no downloads: it checks the original CSV hashes against the pinned source contracts,
reparses the rows, and requires complete 380-match seasons. It does not trust edited
normalized snapshots. The generated report is ignored by Git:
`data/reports/prediction/historical-hda-v1.json`.

For an expanded local Docker demo built from this revision, use the already cached
sources in its persistent corpus volume:

```bash
docker exec pl-knowledge-engine-demo-app-1 .venv/bin/python scripts/evaluate_prediction.py
```

The report includes source URLs/revisions/checksums, a normalized dataset hash,
model/protocol versions, frozen settings, validation metrics and all test forecasts.
Each forecast records its exclusive date cutoff, latest eligible date, history
count, rolling team match IDs, goal rates and outcome probabilities. A lock prevents
concurrent report writes; a failed run replaces previous success with an incomplete
report before checking inputs. `valid: true` means computation and source checks
passed, not that the model achieved a production-quality accuracy threshold.

## Temporal protocol

| Partition | Season | Purpose |
| --- | --- | --- |
| Initial history | 2022–23 | Prior results for the first validation forecast |
| Validation | 2023–24 | Choose a rolling window from 5, 10 or 20 matches |
| Test | 2024–25 | Score the selected model on 380 held-out fixtures |

Selection minimizes validation log loss, breaking exact ties toward the smaller
window. The test season does not select settings. Evaluation walks forward:
earlier validation/test results become available for later fixtures. All results
on the target date are excluded because reliable kickoff ordering is unavailable.
This is not a single frozen preseason forecast of the entire held-out season.

The league baseline uses expanding prior home and away goal averages. The rolling
model multiplies those rates by each team's recent scoring and opponent's conceding
strength, normalized against the expanding league goals-per-team average. Strengths
shrink toward the league average using ten pseudo-matches; unseen clubs therefore
start at neutral strength. Goal rates are clipped to 0.1–8. Independent Poisson goal
distributions, normalized over 0–60 goals, produce home/draw/away probabilities.
The uniform comparator assigns one third to each outcome.

Only dates, team identities and earlier scores enter features. Shots, betting odds,
final tables, player totals, transcripts and the target result do not. Tests mutate
target/same-day/future outcomes and test-season scores to check temporal isolation.

## Measured result

The pinned three-season corpus contains 1,140 fixtures. Protocol
`historical-hda-walk-forward-v1`, model `prematch-poisson-v1`, selected **20 matches**
on validation (log loss 0.960032). The normalized dataset SHA-256 is
`595c5b410b09564b82f8dff2d1b139116551bbec7fa4d3fc09553054b380b9ae`.

| Model | Test log loss ↓ | Test Brier ↓ | Top-outcome accuracy |
| --- | --- | --- | --- |
| Rolling Poisson | 0.992406 | 0.592023 | 51.32% |
| League-average Poisson | 1.081791 | 0.656312 | 40.79% |
| Uniform probabilities | 1.098612 | 0.666667 | 40.79% |

Log loss uses natural logs. Multiclass Brier sums squared errors across the three
outcomes (range 0–2). Top-outcome ties choose home, then draw, then away; uniform
accuracy is therefore the home-win share, not a random-choice simulation.

These are retrospective, result-date-bounded scores on one season. Source CSVs
lack publication timestamps and may include later corrections, so this is not a
reconstruction of the information available at kickoff. The model has no injury,
lineup, odds or tactical inputs; no calibrated uncertainty, betting profitability,
or general forecasting skill is claimed. The probabilities express model outcomes,
not confidence intervals on model accuracy. The [integrated workflow](prediction-workflow.md) extends evaluation to 2025–26
and adds descriptive calibration, approximate score intervals and date-bounded
explanations. Live upcoming-match forecasting remains outside this scope.
