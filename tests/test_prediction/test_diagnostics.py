import pytest

from src.prediction.diagnostics import calibration, intervals


def rows():
    return [
        {
            "match_id": str(i),
            "date": f"2024-08-{i // 2 + 1:02}",
            "outcome": "home",
            "probabilities": {"home": 0.6, "draw": 0.2, "away": 0.2},
        }
        for i in range(6)
    ]


def test_calibration_independent_counts_and_frequencies():
    result = calibration(rows())
    assert sum(b["matches"] for b in result["home"]) == 6
    bucket = next(b for b in result["home"] if b["matches"])
    assert bucket["mean_probability"] == pytest.approx(0.6)
    assert bucket["observed_frequency"] == 1
    assert result["home"][0]["observed_frequency"] is None


def test_date_block_bootstrap_is_paired_deterministic_and_score_only():
    data = rows()
    result = intervals(data, data, repeats=100)
    assert result == intervals(data, data, repeats=100)
    assert result["blocks"] == 3
    assert result["intervals"]["accuracy"] == {"lower": 1, "upper": 1}
    assert result["intervals"]["log_loss_minus_league"] == {"lower": 0, "upper": 0}
    assert "Not forecast intervals" in result["limitation"]
    with pytest.raises(ValueError, match="paired"):
        intervals(data, list(reversed(data)), repeats=100)
    with pytest.raises(ValueError):
        intervals([], [])
