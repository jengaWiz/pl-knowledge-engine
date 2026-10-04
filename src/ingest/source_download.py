"""Bounded public source downloads with verified local caching."""

import os
import time
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests

from config.sources import SourceSpec, inspect_source

MAX_BYTES = 20_000_000


def atomic_write(path: Path, content: bytes) -> None:
    """Publish complete bytes; never leave a partially written cache entry."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        dir=path.parent, prefix=path.name + ".", suffix=".tmp", delete=False
    ) as file:
        temporary = Path(file.name)
        try:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def download_source(
    source: SourceSpec, cache: Path, *, refresh: bool = False, session=None
) -> tuple[bytes, bool]:
    """Return verified content and whether it came from cache.

    Retry transient HTTP/network failures at most three times. Changed source
    content requires contract review rather than silently accepting a new corpus.
    """
    if cache.exists() and not refresh:
        content = cache.read_bytes()
        inspect_source(source, content)
        return content, True
    owned_session = session is None
    session = session or requests.Session()
    try:
        session.headers["User-Agent"] = "PL-Knowledge-Engine/0.1 (public dataset collection)"
        for attempt in range(3):
            try:
                with session.get(source.url, timeout=(10, 30), stream=True) as response:
                    response.raise_for_status()
                    chunks = []
                    size = 0
                    for chunk in response.iter_content(65536):
                        size += len(chunk)
                        if size > MAX_BYTES:
                            raise ValueError(f"{source.id}: source exceeds {MAX_BYTES} bytes")
                        chunks.append(chunk)
                content = b"".join(chunks)
                inspect_source(source, content)
                atomic_write(cache, content)
                return content, False
            except requests.RequestException as exc:
                status = exc.response.status_code if exc.response is not None else None
                if attempt == 2 or (status is not None and status < 500 and status != 429):
                    raise
                time.sleep(2**attempt)
        raise AssertionError("Unreachable retry state")
    finally:
        if owned_session:
            session.close()
