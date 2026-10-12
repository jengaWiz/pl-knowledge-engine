"""Descriptive calibration and approximate date-block bootstrap score intervals."""

import math
import random
from collections import defaultdict


def calibration(rows):
    result = {}
    for kind in ("home", "draw", "away"):
        bins = [[] for _ in range(5)]
        for row in rows:
            p = row["probabilities"][kind]
            bins[min(4, int(p * 5))].append((p, int(row["outcome"] == kind)))
        result[kind] = [
            {
                "lower": i / 5,
                "upper": (i + 1) / 5,
                "matches": len(group),
                "mean_probability": sum(p for p, _ in group) / len(group) if group else None,
                "observed_frequency": sum(y for _, y in group) / len(group) if group else None,
            }
            for i, group in enumerate(bins)
        ]
    return result


def intervals(rows, baseline, *, repeats=500):
    if not rows or len(rows) != len(baseline) or repeats < 100:
        raise ValueError("Paired predictions and at least 100 bootstrap repetitions required")
    blocks = defaultdict(list)

    def losses(row):
        p, target = row["probabilities"], row["outcome"]
        return (
            -math.log(max(p[target], 1e-15)),
            sum((value - (kind == target)) ** 2 for kind, value in p.items()),
            float(max(p, key=p.get) == target),
        )

    for row, other in zip(rows, baseline, strict=True):
        if any(row[key] != other[key] for key in ("match_id", "date", "outcome")):
            raise ValueError("Bootstrap fixtures must be paired")
        own, ref = losses(row), losses(other)
        blocks[row["date"]].append((*own, own[0] - ref[0], own[1] - ref[1]))
    days = sorted(blocks)
    rng = random.Random(1729)
    samples = [[] for _ in range(5)]
    for _ in range(repeats):
        selected = [item for day in rng.choices(days, k=len(days)) for item in blocks[day]]
        for i, sample in enumerate(samples):
            sample.append(sum(row[i] for row in selected) / len(selected))
    names = ("log_loss", "brier", "accuracy", "log_loss_minus_league", "brier_minus_league")
    return {
        "method": "date-block percentile bootstrap",
        "seed": 1729,
        "repetitions": repeats,
        "blocks": len(days),
        "level": 0.95,
        "intervals": {
            name: {
                "lower": sorted(values)[int(0.025 * repeats)],
                "upper": sorted(values)[min(repeats - 1, int(0.975 * repeats))],
            }
            for name, values in zip(names, samples, strict=True)
        },
        "limitation": "Approximate within-season score uncertainty; date blocks do not capture "
        "all serial dependence or season-to-season drift. Not forecast intervals.",
    }
