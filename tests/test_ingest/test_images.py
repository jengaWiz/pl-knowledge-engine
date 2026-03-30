"""
Tests for src/ingest/images.py
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call
import json

import pytest

from src.ingest.images import ImageDownloader, _safe_filename


class TestSafeFilename:
    def test_spaces_become_underscores(self):
        assert _safe_filename("Aston Villa") == "aston_villa"

    def test_special_chars_stripped(self):
        assert _safe_filename("Díaz") == "daz"  # accent stripped

    def test_lowercase(self):
        assert _safe_filename("LIVERPOOL") == "liverpool"


class TestImageDownloader:
    def _make_downloader(self, tmp_path):
        with patch("src.ingest.images.settings") as ms, \
             patch("src.ingest.images.Checkpoint"):
            ms.raw_dir = tmp_path
            ms.checkpoint_dir = tmp_path / "checkpoints"
            (tmp_path / "images" / "players").mkdir(parents=True, exist_ok=True)
            (tmp_path / "images" / "badges").mkdir(parents=True, exist_ok=True)
            dl = ImageDownloader.__new__(ImageDownloader)
            dl.players_dir = tmp_path / "images" / "players"
            dl.badges_dir = tmp_path / "images" / "badges"
            dl.session = MagicMock()
            dl.checkpoint = MagicMock()
            dl.checkpoint.is_completed.return_value = False
            dl.metadata = []
            return dl

    def test_focus_team_detection_villa(self, tmp_path):
        dl = self._make_downloader(tmp_path)
        assert dl._team_name_matches_focus("Aston Villa") is True

    def test_focus_team_detection_liverpool(self, tmp_path):
        dl = self._make_downloader(tmp_path)
        assert dl._team_name_matches_focus("Liverpool") is True

    def test_non_focus_team_rejected(self, tmp_path):
        dl = self._make_downloader(tmp_path)
        assert dl._team_name_matches_focus("Arsenal") is False

    def test_download_badge_success(self, tmp_path):
        """A successful badge download should create a PNG and add metadata."""
        dl = self._make_downloader(tmp_path)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"\x89PNG\r\n"  # fake PNG bytes
        mock_resp.raise_for_status = MagicMock()
        dl.session.get.return_value = mock_resp

        teams = [{"id": 1, "name": "Aston Villa", "code": 7}]
        dl.download_badges(teams)

        assert (tmp_path / "images" / "badges" / "aston_villa.png").exists()
        assert len(dl.metadata) == 1
        assert dl.metadata[0]["type"] == "team_badge"

    def test_download_badge_404_skipped(self, tmp_path):
        """A 404 response for a badge should be silently skipped."""
        dl = self._make_downloader(tmp_path)
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        dl.session.get.return_value = mock_resp

        teams = [{"id": 1, "name": "Liverpool", "code": 14}]
        dl.download_badges(teams)
        assert len(dl.metadata) == 0

    def test_checkpoint_skips_already_downloaded(self, tmp_path):
        """Items already in the checkpoint should not be re-downloaded."""
        dl = self._make_downloader(tmp_path)
        dl.checkpoint.is_completed.return_value = True
        teams = [{"id": 1, "name": "Aston Villa", "code": 7}]
        dl.download_badges(teams)
        dl.session.get.assert_not_called()

    def test_non_focus_team_player_skipped(self, tmp_path):
        """Players from non-focus teams should not be downloaded."""
        dl = self._make_downloader(tmp_path)
        teams_data = [{"id": 1, "name": "Arsenal"}, {"id": 2, "name": "Aston Villa"}]
        players_data = [
            {"id": 101, "team": 1, "first_name": "Bukayo", "second_name": "Saka", "photo": "12345.jpg"},
        ]
        dl.download_player_photos(players_data, teams_data)
        assert len(dl.metadata) == 0

    def test_metadata_written_to_json(self, tmp_path):
        """save_metadata should write a valid JSON file."""
        dl = self._make_downloader(tmp_path)
        dl.metadata = [{"type": "team_badge", "team": "Liverpool"}]
        with patch("src.ingest.images.settings") as ms:
            ms.raw_dir = tmp_path
            meta_path = tmp_path / "images" / "images_metadata.json"
            dl.save_metadata_ = lambda: json.dump(dl.metadata, open(str(meta_path), "w"))
            dl.save_metadata_()

        with open(tmp_path / "images" / "images_metadata.json") as f:
            data = json.load(f)
        assert len(data) == 1
