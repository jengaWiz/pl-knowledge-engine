"""Reject changed content, incorrect schemas and mislabeled seasons."""

import hashlib
from dataclasses import replace

import pytest

from config.sources import inspect_source, load_sources


def source_for(content):
    return replace(load_sources("2025-26")[0], sha256=hashlib.sha256(content).hexdigest())


def test_changed_source_requires_review():
    with pytest.raises(ValueError, match="checksum changed"):
        inspect_source(load_sources("2025-26")[0], b"changed upstream content")


def test_missing_schema_is_rejected():
    content = b"Div,Date\nE0,15/08/2025\n"
    with pytest.raises(ValueError, match="missing columns"):
        inspect_source(source_for(content), content)


def test_date_evidence_overrides_source_label():
    content = b"Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR\nE0,15/08/2026,Villa,Liverpool,1,0,H\n"
    with pytest.raises(ValueError, match="configured season"):
        inspect_source(source_for(content), content)


def test_match_source_reports_dates_and_teams():
    content = b"Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR\nE0,15/08/2025,Villa,Liverpool,1,0,H\n"
    report = inspect_source(source_for(content), content)
    assert report["rows"] == 1
    assert report["date_range"] == ["2025-08-15", "2025-08-15"]
    assert report["teams"] == ["Liverpool", "Villa"]


def test_unknown_season_does_not_fall_back_to_live():
    with pytest.raises(ValueError, match="No verified source contracts"):
        load_sources("2026-27")
