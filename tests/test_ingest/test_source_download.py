"""Verified caches and failed transfers must not publish untrusted data."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from config.sources import load_sources
from src.ingest.source_download import MAX_BYTES, download_source


def test_verified_cache_avoids_network(tmp_path):
    cache = tmp_path / "matches.csv"
    cache.write_bytes(b"cached")
    session = MagicMock()
    with patch("src.ingest.source_download.inspect_source") as inspect:
        assert download_source(load_sources("2025-26")[0], cache, session=session) == (
            b"cached",
            True,
        )
    inspect.assert_called_once()
    session.get.assert_not_called()


def test_corrupt_cache_fails_explicitly_without_network(tmp_path):
    cache = tmp_path / "matches.csv"
    cache.write_bytes(b"untrusted")
    session = MagicMock()
    with pytest.raises(ValueError, match="checksum changed"):
        download_source(load_sources("2025-26")[0], cache, session=session)
    session.get.assert_not_called()


def test_transient_failures_retry_at_most_three_times(tmp_path):
    session = MagicMock()
    session.get.side_effect = requests.Timeout("timeout")
    with patch("src.ingest.source_download.time.sleep") as sleep:
        with pytest.raises(requests.Timeout):
            download_source(load_sources("2025-26")[0], tmp_path / "matches.csv", session=session)
    assert session.get.call_count == 3
    assert sleep.call_count == 2
    assert not (tmp_path / "matches.csv").exists()


def test_permanent_http_failure_is_not_retried(tmp_path):
    session = MagicMock()
    response = MagicMock(status_code=404)
    session.get.side_effect = requests.HTTPError("not found", response=response)
    with pytest.raises(requests.HTTPError):
        download_source(load_sources("2025-26")[0], tmp_path / "matches.csv", session=session)
    assert session.get.call_count == 1


def test_oversized_download_is_not_published(tmp_path):
    session = MagicMock()
    response = session.get.return_value.__enter__.return_value
    response.iter_content.return_value = [b"x" * (MAX_BYTES + 1)]
    with pytest.raises(ValueError, match="exceeds"):
        download_source(load_sources("2025-26")[0], tmp_path / "matches.csv", session=session)
    assert not (tmp_path / "matches.csv").exists()
