"""Public metadata parsing, season filtering, provenance and honest fallback."""

import json

import pytest

from src.ingest import commentary

RSS = b"""<?xml version="1.0"?><rss><channel><item>
<title>Liverpool match reaction</title><link>https://www.liverpoolfc.com/news/reaction</link>
<pubDate>Sun, 01 Mar 2026 12:00:00 GMT</pubDate></item></channel></rss>"""


@pytest.fixture
def collector(tmp_path, monkeypatch):
    monkeypatch.setattr(commentary, "load_verified_corpus", lambda *args: {})
    return tmp_path


def test_rss_retains_link_date_and_title_only():
    rows, feeds = commentary.parse_metadata(RSS, "https://www.liverpoolfc.com/news")
    assert rows[0]["url"] == "https://www.liverpoolfc.com/news/reaction"
    assert rows[0]["published_at"] == "Sun, 01 Mar 2026 12:00:00 GMT"
    assert not feeds and "text" not in rows[0]


def test_atom_caption_episode_metadata_without_media_download():
    content = b"""<feed xmlns="http://www.w3.org/2005/Atom"><entry>
    <title>Match review</title><link href="https://www.youtube.com/watch?v=abcdef12345"/>
    <published>2026-03-01T12:00:00Z</published></entry></feed>"""
    rows, _ = commentary.parse_metadata(content, "https://www.youtube.com/feeds/videos.xml")
    assert rows[0]["url"].endswith("abcdef12345")


def test_html_doctype_and_jsonld_metadata():
    content = b"""<!DOCTYPE html><html><script type="application/ld+json">
    {"@type":"NewsArticle","headline":"Match reaction","datePublished":"2026-03-01",
    "url":"https://www.avfc.co.uk/news/reaction"}</script></html>"""
    rows, _ = commentary.parse_metadata(content, "https://www.avfc.co.uk/news/")
    assert rows[0]["title"] == "Match reaction"


def test_xml_entity_declarations_rejected():
    with pytest.raises(ValueError, match="XML declarations"):
        commentary.parse_metadata(b'<!DOCTYPE x [<!ENTITY a "x">]><rss/>', "https://www.avfc.co.uk")


def test_collector_deduplicates_and_marks_external_metadata(collector, monkeypatch):
    monkeypatch.setattr(commentary, "fetch_metadata", lambda *args: RSS)
    report = commentary.collect_commentary("2025-26", collector)
    rows, digest = commentary.load_commentary(collector, "2025-26")
    assert report["items"] == 1 and report["coverage"] == {"Aston Villa": 1, "Liverpool": 1}
    assert rows[0]["teams"] == ["Aston Villa", "Liverpool"]
    assert rows[0]["evidence_type"] == "publisher_metadata"
    assert digest == report["artifact_sha256"]
    artifact = collector / "cleaned/mvp/2025-26/commentary.jsonl"
    artifact.write_text(artifact.read_text() + "\n")
    with pytest.raises(ValueError, match="changed"):
        commentary.load_commentary(collector, "2025-26")


def test_access_failure_records_zero_coverage_and_fallback(collector, monkeypatch):
    def fail(*args):
        raise ValueError("Robots policy disallows this source")

    monkeypatch.setattr(commentary, "fetch_metadata", fail)
    report = commentary.collect_commentary("2025-26", collector)
    assert report["valid"] and report["external_commentary"] == "unavailable"
    assert report["coverage"] == {"Aston Villa": 0, "Liverpool": 0}
    assert all(item["status"] == "unavailable" for item in report["attempts"])
    assert "statistics" in report["fallback"]


def test_out_of_season_items_rejected(collector, monkeypatch):
    monkeypatch.setattr(commentary, "fetch_metadata", lambda *args: RSS.replace(b"2026", b"2027"))
    report = commentary.collect_commentary("2025-26", collector)
    assert report["items"] == 0 and all(item["rejected"] == 1 for item in report["attempts"])


@pytest.mark.parametrize(
    "url",
    [
        "http://www.avfc.co.uk/news",
        "https://localhost/news",
        "https://www.avfc.co.uk:443/news",
        "https://user@www.avfc.co.uk/news",
    ],
)
def test_manifest_validation_precedes_network(collector, monkeypatch, url):
    manifest = collector / "manifest.json"
    manifest.write_text(json.dumps([{"url": url, "team": "Aston Villa", "publisher": "Villa"}]))
    monkeypatch.setattr(commentary, "fetch_metadata", lambda *args: pytest.fail("Network called"))
    with pytest.raises(ValueError, match="HTTPS"):
        commentary.collect_commentary("2025-26", collector, manifest=manifest)


def test_robots_disallow_prevents_content_request():
    class Response:
        status_code = 200
        text = "User-agent: *\nDisallow: /news"
        content = text.encode()

    class Session:
        def get(self, url, **kwargs):
            assert url == "https://www.avfc.co.uk/robots.txt"
            assert kwargs["allow_redirects"] is False
            return Response()

    with pytest.raises(ValueError, match="disallows"):
        commentary.fetch_metadata("https://www.avfc.co.uk/news", Session())


def test_source_byte_limit_is_enforced(monkeypatch):
    monkeypatch.setattr(commentary, "MAX_BYTES", 100)

    class Response:
        status_code = 200
        text = "User-agent: *\nAllow: /"
        content = text.encode()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def iter_content(self, size):
            yield b"x" * 101

    class Session:
        def get(self, url, **kwargs):
            return Response()

    with pytest.raises(ValueError, match="size limit"):
        commentary.fetch_metadata("https://www.avfc.co.uk/news", Session())
