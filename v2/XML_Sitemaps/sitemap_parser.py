"""
Sitemap loader for YC properties with local/remote toggle and subset filtering.
"""

from __future__ import annotations

import httpx
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.error import URLError

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from Python_Scraping.core.logging import console
from Python_Scraping.core.models import SitemapEntryModel
from Python_Scraping.core.store import SQLiteStore, default_store

# XML namespace for sitemaps
NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


@dataclass
class SitemapSource:
    name: str
    url: str
    local_path: Optional[Path] = None


@dataclass
class SitemapEntry:
    url: str
    lastmod: Optional[str]
    source: str


SITEMAP_SOURCES = [
    SitemapSource(
        name="main",
        url="https://www.ycombinator.com/sitemap.xml",
        local_path=Path(__file__).with_name("sitemap.xml"),
    ),
    SitemapSource(
        name="companies",
        url="https://www.ycombinator.com/companies/sitemap",
        local_path=Path(__file__).with_name("companies_sitemap.xml"),
    ),
    SitemapSource(
        name="library",
        url="https://www.ycombinator.com/library/sitemap.xml",
        local_path=Path(__file__).with_name("library_sitemap.xml"),
    ),
    SitemapSource(
        name="launches",
        url="https://www.ycombinator.com/launches/sitemap",
        local_path=Path(__file__).with_name("launches_sitemap.xml"),
    ),
    SitemapSource(
        name="jobs",
        url="https://www.ycombinator.com/jobs/sitemap",
        local_path=Path(__file__).with_name("jobs_sitemap.xml"),
    ),
]


class SitemapParser:
    """Parse sitemap xml (urlset or sitemapindex)."""

    def parse(self, xml_content: str, source: str) -> List[SitemapEntry]:
        root = ET.fromstring(xml_content)
        tag = root.tag.lower()
        if tag.endswith("sitemapindex"):
            urls: List[SitemapEntry] = []
            for child in root.findall("sm:sitemap", NS):
                loc = child.find("sm:loc", NS)
                if loc is not None and loc.text:
                    nested = self._fetch_nested(loc.text.strip(), source=source)
                    urls.extend(nested)
            return urls
        return self._parse_urlset(root, source=source)

    def _parse_urlset(self, root, *, source: str) -> List[SitemapEntry]:
        urls: List[SitemapEntry] = []
        for url_elem in root.findall("sm:url", NS):
            loc = url_elem.find("sm:loc", NS)
            lastmod = url_elem.find("sm:lastmod", NS)
            if loc is not None and loc.text:
                urls.append(
                    SitemapEntry(
                        url=loc.text.strip(),
                        lastmod=lastmod.text.strip() if lastmod is not None else None,
                        source=source,
                    )
                )
        return urls

    def _fetch_nested(self, url: str, *, source: str) -> List[SitemapEntry]:
        try:
            xml = fetch_sitemap(url)
            if xml is None:
                return []
            return self.parse(xml, source=source)
        except Exception as exc:
            console.print(f"  Error fetching nested sitemap {url}: {exc}")
            return []


class SitemapLoader:
    """Glue code to fetch, parse, and insert sitemap URLs."""

    def __init__(
        self, store: SQLiteStore = default_store, parser: Optional[SitemapParser] = None
    ) -> None:
        self.store = store
        self.parser = parser or SitemapParser()

    def load_source(self, source: SitemapSource, *, use_local: bool = True) -> int:
        mode = "local" if use_local else "remote"
        console.print(f"[cyan]Fetching sitemap: {source.name} ({mode})[/]")
        xml_content = self._get_content(source, use_local=use_local)
        if xml_content is None:
            console.print(f"  Skipping {source.name}; no content")
            return 0
        entries = self.parser.parse(xml_content, source=source.name)
        console.print(f"  Found {len(entries)} URLs")
        inserted = self._insert_urls(entries)
        console.print(f"  Inserted {inserted} new URLs")
        return inserted

    def load_all(
        self, *, use_local: bool = True, only: Optional[Iterable[str]] = None
    ) -> int:
        names = {n for n in (only or [])}
        sources = [s for s in SITEMAP_SOURCES if not names or s.name in names]

        total_inserted = 0
        for src in sources:
            total_inserted += self.load_source(src, use_local=use_local)
        console.print(f"\n[green]Total new URLs inserted: {total_inserted}[/]")
        return total_inserted

    def _get_content(self, source: SitemapSource, *, use_local: bool) -> Optional[str]:
        if use_local and source.local_path and source.local_path.exists():
            return source.local_path.read_text(encoding="utf-8")
        return fetch_sitemap(source.url)

    def _insert_urls(self, entries: List[SitemapEntry]) -> int:
        if not entries:
            return 0
        models = [
            SitemapEntryModel(url=e.url, lastmod=e.lastmod, source=e.source)
            for e in entries
        ]
        return self.store.insert_sitemap_entries(models)


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=8),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((URLError, httpx.HTTPError)),
    reraise=True,
)
def fetch_sitemap(url: str) -> Optional[str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/xml,application/xml,*/*",
    }
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.text


def get_stats(store: SQLiteStore = default_store) -> dict:
    with store.connect() as conn:
        cursor = conn.cursor()
        stats = {}
        cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap")
        stats["total"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap WHERE visited = 1")
        stats["visited"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap WHERE visited = 0")
        stats["unvisited"] = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT sitemap_source, COUNT(*) as count
            FROM websites_from_sitemap
            GROUP BY sitemap_source
        """
        )
        stats["by_source"] = {
            row["sitemap_source"]: row["count"] for row in cursor.fetchall()
        }
    return stats
