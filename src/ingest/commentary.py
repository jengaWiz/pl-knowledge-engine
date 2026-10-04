"""Bounded public metadata collection; unavailable commentary is explicit."""

import hashlib
import json
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from urllib.robotparser import RobotFileParser
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup

from config.season import season_bounds
from src.clean.corpus_quality import load_verified_corpus
from src.ingest.source_download import atomic_write

FOCUS = ("Aston Villa", "Liverpool")
HOSTS = {
    "www.avfc.co.uk",
    "avfc.co.uk",
    "www.liverpoolfc.com",
    "liverpoolfc.com",
    "www.youtube.com",
    "youtube.com",
}
DEFAULT_SOURCES = [
    {"url": "https://www.avfc.co.uk/news/", "team": "Aston Villa", "publisher": "Aston Villa FC"},
    {"url": "https://www.liverpoolfc.com/news", "team": "Liverpool", "publisher": "Liverpool FC"},
]
MAX_BYTES = 2_000_000
AGENT = "PLKnowledgeEngine/0.2 (public metadata only)"


def public_url(url):
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname not in HOSTS or parts.username or parts.port:
        raise ValueError("Source must use HTTPS on an official club or YouTube host")
    return url


def fetch_metadata(url, session):
    """Respect robots; no redirects, authentication or access-control bypass."""
    public_url(url)
    parts = urlsplit(url)
    robots_url = f"https://{parts.netloc}/robots.txt"
    robot_response = session.get(robots_url, timeout=(10, 20), allow_redirects=False)
    if robot_response.status_code != 200:
        raise ValueError(f"Robots policy unavailable ({robot_response.status_code})")
    if len(robot_response.content) > MAX_BYTES:
        raise ValueError("Robots policy exceeds size limit")
    policy = RobotFileParser(robots_url)
    policy.parse(robot_response.text.splitlines())
    if not policy.can_fetch(AGENT, url):
        raise ValueError("Robots policy disallows this source")
    with session.get(url, timeout=(10, 20), allow_redirects=False, stream=True) as response:
        if response.status_code != 200:
            raise ValueError(f"Public source unavailable ({response.status_code})")
        chunks = []
        size = 0
        for chunk in response.iter_content(65536):
            size += len(chunk)
            if size > MAX_BYTES:
                raise ValueError("Public source exceeds size limit")
            chunks.append(chunk)
    return b"".join(chunks)


def parse_metadata(content, base_url):
    """Retain titles, URLs and dates only, never whole articles or transcripts."""
    records = []
    if b"<!ENTITY" in content.upper() or (
        b"<!DOCTYPE" in content.upper() and b"<!DOCTYPE HTML>" not in content.upper()
    ):
        raise ValueError("Unsupported XML declarations")
    try:
        tree = ElementTree.fromstring(content)
    except ElementTree.ParseError:
        tree = None
    if tree is not None:
        for item in tree.iter():
            if item.tag.split("}")[-1] not in ("item", "entry"):
                continue
            fields = {child.tag.split("}")[-1].lower(): child for child in item}
            title = fields.get("title")
            link = fields.get("link")
            day = next(
                (fields[key] for key in ("pubdate", "published", "updated") if key in fields), None
            )
            if title is not None and link is not None and day is not None:
                records.append(
                    {
                        "title": title.text or "",
                        "url": link.get("href") or link.text or "",
                        "published_at": day.text or "",
                    }
                )
    if tree is not None and tree.tag.split("}")[-1] in ("rss", "feed"):
        return records, []
    soup = BeautifulSoup(content, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or script.get_text())
        except (ValueError, TypeError):
            continue
        pending = [data]
        while pending:
            value = pending.pop()
            if isinstance(value, list):
                pending.extend(value)
            elif isinstance(value, dict):
                if value.get("@type") in ("NewsArticle", "Article", "VideoObject"):
                    records.append(
                        {
                            "title": value.get("headline") or value.get("name", ""),
                            "url": value.get("url") or base_url,
                            "published_at": value.get("datePublished")
                            or value.get("uploadDate", ""),
                        }
                    )
                pending.extend(v for v in value.values() if isinstance(v, (list, dict)))
    feeds = [
        urljoin(base_url, link.get("href", ""))
        for link in soup.find_all("link", rel="alternate")
        if link.get("type") in ("application/rss+xml", "application/atom+xml")
    ]
    return records, feeds[:2]


def normalize(record, source, season, source_hash):
    title = " ".join(record.get("title", "").split())[:300]
    url = public_url(urljoin(source["url"], record.get("url", "")))
    value = record.get("published_at", "")
    try:
        day = datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        day = parsedate_to_datetime(value).date()
    lower, upper = season_bounds(season)
    if not title or not lower <= day < upper:
        raise ValueError("Metadata title/date is missing or outside the configured season")
    return {
        "id": "commentary:" + hashlib.sha256(url.encode()).hexdigest()[:24],
        "season": season,
        "title": title,
        "url": url,
        "published_at": day.isoformat(),
        "publisher": source["publisher"],
        "teams": [source["team"]],
        "evidence_type": "publisher_metadata",
        "source_url": source["url"],
        "source_sha256": source_hash,
        "caption_timestamps": [],
        "limitation": "Title metadata only; external opinion is not measured statistics.",
    }


def collect_commentary(
    season: str, output: Path, *, manifest: Path | None = None, session=None
) -> dict:
    load_verified_corpus(output, season)
    sources = json.loads(manifest.read_text()) if manifest else DEFAULT_SOURCES
    if not isinstance(sources, list) or len(sources) > 20:
        raise ValueError("Curated manifest must contain at most 20 sources")
    # Validate the complete manifest before contacting any source.
    for source in sources:
        public_url(source["url"])
        if source["team"] not in FOCUS or not source.get("publisher"):
            raise ValueError("Manifest requires a focus team and publisher")
    session = session or requests.Session()
    session.headers.update({"User-Agent": AGENT})
    records = {}
    attempts = []
    for source in sources:
        queue = [source["url"]]
        seen = set()
        while queue and len(seen) < 3:
            url = queue.pop(0)
            if url in seen:
                continue
            seen.add(url)
            attempt = {"url": url, "team": source["team"], "accepted": 0, "rejected": 0}
            try:
                content = fetch_metadata(url, session)
                digest = hashlib.sha256(content).hexdigest()
                parsed, feeds = parse_metadata(content, url)
                # Curated publication metadata can accompany a publicly accessible URL.
                if url == source["url"] and source.get("title"):
                    parsed.append(source)
                for item in parsed:
                    try:
                        row = normalize(item, {**source, "url": url}, season, digest)
                    except (ValueError, TypeError, AttributeError, OverflowError):
                        attempt["rejected"] += 1
                        continue
                    if sum(source["team"] in r["teams"] for r in records.values()) >= 10:
                        break
                    if row["id"] in records:
                        existing = records[row["id"]]
                        existing["teams"] = sorted(set(existing["teams"] + row["teams"]))
                    else:
                        records[row["id"]] = row
                        attempt["accepted"] += 1
                queue.extend(feeds)
                attempt["status"] = "accessible"
                attempt["source_sha256"] = digest
            except (requests.RequestException, ValueError) as exc:
                attempt.update(status="unavailable", error=str(exc))
            attempts.append(attempt)
    payload = "".join(
        json.dumps(row, sort_keys=True) + "\n"
        for row in sorted(records.values(), key=lambda row: row["id"])
    ).encode()
    report = {
        "valid": True,
        "season": season,
        "target_per_team": 10,
        "coverage": {team: sum(team in r["teams"] for r in records.values()) for team in FOCUS},
        "attempts": attempts,
        "artifact_sha256": hashlib.sha256(payload).hexdigest(),
        "items": len(records),
        "fallback": "Verified statistics-derived summaries",
        "external_commentary": "available" if records else "unavailable",
        "caption_status": "No captions collected; no audio/video downloads or provider calls",
    }
    atomic_write(output / "cleaned/mvp" / season / "commentary.jsonl", payload)
    atomic_write(
        output / "reports/mvp" / season / "commentary.json",
        (json.dumps(report, indent=2) + "\n").encode(),
    )
    return report


def load_commentary(output: Path, season: str) -> tuple[list[dict], str]:
    report_path = output / "reports/mvp" / season / "commentary.json"
    artifact = output / "cleaned/mvp" / season / "commentary.jsonl"
    if not report_path.exists() and not artifact.exists():
        return [], "uncollected"
    report = json.loads(report_path.read_text())
    content = artifact.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if not report.get("valid") or report["season"] != season or digest != report["artifact_sha256"]:
        raise ValueError("Commentary artifact is unverified or changed")
    return [json.loads(line) for line in content.splitlines()], digest
