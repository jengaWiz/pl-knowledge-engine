"""CLI provenance gate uses original pinned rows and withdraws stale success."""

import csv
import io
import json

import pytest
import requests

from config.sources import SourceSpec
from scripts import evaluate_prediction as cli
from src.corpus.contracts import sha256
from src.ingest.match_history import raw_path


def test_cached_sources_repeat_and_checksum_failure(tmp_path, monkeypatch):
    sources = []
    teams = ["Aston Villa", "Liverpool", *[f"Club {i}" for i in range(18)]]
    fields = ["Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]
    for year in (2022, 2023, 2024):
        season = f"{year}-{(year + 1) % 100:02d}"
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(fields)
        for home in teams:
            for away in teams:
                if home != away:
                    writer.writerow(["E0", f"01/09/{year}", home, away, 1, 0, "H"])
        content = buffer.getvalue().encode()
        checksum = sha256(content)
        url = (
            f"https://www.football-data.co.uk/mmz4281/{year % 100:02d}{(year + 1) % 100:02d}/E0.csv"
        )
        sources.append(
            SourceSpec(
                "matches",
                season,
                url,
                "Publisher",
                "https://www.football-data.co.uk/englandm.php",
                "Retain attribution",
                "sha256:" + checksum,
                checksum,
                fields,
                "Test",
            )
        )
        path = raw_path(tmp_path, season)
        path.parent.mkdir(parents=True)
        path.write_bytes(content)
    monkeypatch.setattr(cli, "load_history_sources", lambda: sources)
    monkeypatch.setattr(requests.Session, "get", lambda *a, **kw: pytest.fail("Network used"))
    first, path = cli.run(tmp_path)
    assert first["valid"] and len(first["test_predictions"]) == 380
    assert first["test_metrics"]["rolling_poisson"]["matches"] == 380
    assert first["sources"][0]["sha256"] == sources[0].sha256
    assert all(p["history_matches"] == 760 for p in first["test_predictions"])
    before = path.read_bytes()
    assert cli.run(tmp_path)[0] == first
    assert path.read_bytes() == before
    raw_path(tmp_path, "2024-25").write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksum"):
        cli.run(tmp_path)
    assert json.loads(path.read_text()) == {"valid": False, "status": "incomplete"}
