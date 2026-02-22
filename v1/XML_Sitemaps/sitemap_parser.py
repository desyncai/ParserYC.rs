"""
Sitemap parser for YC website.
Fetches and parses sitemaps, then stores URLs in the database.
"""

import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

from Sqlite_Database.schema import create_tables, get_connection

# YC Sitemap URLs
SITEMAPS = {
    "main": "https://www.ycombinator.com/sitemap.xml",
    "companies": "https://www.ycombinator.com/companies/sitemap",
    "library": "https://www.ycombinator.com/library/sitemap.xml",
    "launches": "https://www.ycombinator.com/launches/sitemap",
    "jobs": "https://www.ycombinator.com/jobs/sitemap",
}

# XML namespace for sitemaps
NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def fetch_sitemap(url: str) -> Optional[str]:
    """Fetch sitemap XML content from URL."""
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/xml,application/xml,*/*",
    }
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=30) as response:
            return response.read().decode("utf-8")
    except URLError as e:
        print(f"Error fetching {url}: {e}")
        return None


def parse_sitemap(xml_content: str) -> list[dict]:
    """
    Parse sitemap XML and extract URLs with lastmod dates.
    Returns list of dicts with 'url' and 'lastmod' keys.
    """
    urls = []
    try:
        root = ET.fromstring(xml_content)
        for url_elem in root.findall("sm:url", NS):
            loc = url_elem.find("sm:loc", NS)
            lastmod = url_elem.find("sm:lastmod", NS)
            if loc is not None and loc.text:
                urls.append(
                    {
                        "url": loc.text.strip(),
                        "lastmod": lastmod.text.strip()
                        if lastmod is not None
                        else None,
                    }
                )
    except ET.ParseError as e:
        print(f"Error parsing sitemap XML: {e}")
    return urls


def insert_urls(urls: list[dict], sitemap_source: str, conn: sqlite3.Connection):
    """Insert URLs into the database, ignoring duplicates."""
    cursor = conn.cursor()
    inserted = 0
    for url_data in urls:
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO websites_from_sitemap (url, lastmod, sitemap_source)
                VALUES (?, ?, ?)
            """,
                (url_data["url"], url_data["lastmod"], sitemap_source),
            )
            if cursor.rowcount > 0:
                inserted += 1
        except sqlite3.Error as e:
            print(f"Error inserting {url_data['url']}: {e}")
    conn.commit()
    return inserted


def load_sitemap(name: str, url: str, conn: sqlite3.Connection) -> int:
    """Fetch, parse, and load a single sitemap into the database."""
    print(f"Fetching sitemap: {name} ({url})")
    xml_content = fetch_sitemap(url)
    if xml_content is None:
        return 0

    urls = parse_sitemap(xml_content)
    print(f"  Found {len(urls)} URLs")

    inserted = insert_urls(urls, name, conn)
    print(f"  Inserted {inserted} new URLs")
    return inserted


def load_all_sitemaps():
    """Load all YC sitemaps into the database."""
    create_tables()
    conn = get_connection()

    total_inserted = 0
    for name, url in SITEMAPS.items():
        inserted = load_sitemap(name, url, conn)
        total_inserted += inserted

    # Get total count
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap")
    total = cursor.fetchone()[0]

    conn.close()
    print(f"\nTotal URLs in database: {total}")
    print(f"New URLs inserted this run: {total_inserted}")
    return total


def get_unvisited_urls(limit: int = 100) -> list[tuple[int, str]]:
    """Get unvisited URLs from the database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, url FROM websites_from_sitemap
        WHERE visited = 0
        LIMIT ?
    """,
        (limit,),
    )
    urls = [(row["id"], row["url"]) for row in cursor.fetchall()]
    conn.close()
    return urls


def mark_as_visited(url_ids: list[int]):
    """Mark URLs as visited."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        UPDATE websites_from_sitemap
        SET visited = 1, visited_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """,
        [(uid,) for uid in url_ids],
    )
    conn.commit()
    conn.close()


def get_stats() -> dict:
    """Get statistics about the sitemap URLs."""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}
    cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap")
    stats["total"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap WHERE visited = 1")
    stats["visited"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap WHERE visited = 0")
    stats["unvisited"] = cursor.fetchone()[0]

    cursor.execute("""
        SELECT sitemap_source, COUNT(*) as count 
        FROM websites_from_sitemap 
        GROUP BY sitemap_source
    """)
    stats["by_source"] = {
        row["sitemap_source"]: row["count"] for row in cursor.fetchall()
    }

    conn.close()
    return stats


def load_from_local_files():
    """Load sitemaps from local files in data/ directory."""

    create_tables()
    conn = get_connection()

    data_dir = Path(__file__).resolve().parent
    local_sitemaps = {
        "companies": data_dir / "companies_sitemap.xml",
        "launches": data_dir / "launches_sitemap.xml",
        "jobs": data_dir / "jobs_sitemap.xml",
    }

    total_inserted = 0
    for name, filepath in local_sitemaps.items():
        if filepath.exists():
            print(f"Loading {name} from {filepath}")
            with open(filepath, "r") as f:
                xml_content = f.read()
            urls = parse_sitemap(xml_content)
            print(f"  Found {len(urls)} URLs")
            inserted = insert_urls(urls, name, conn)
            print(f"  Inserted {inserted} new URLs")
            total_inserted += inserted
        else:
            print(f"File not found: {filepath}")

    conn.close()
    print(f"\nTotal new URLs inserted: {total_inserted}")
    return total_inserted


if __name__ == "__main__":
    import sys

    if "--local" in sys.argv:
        load_from_local_files()
    else:
        load_all_sitemaps()
    print("\nStats:")
    for key, value in get_stats().items():
        print(f"  {key}: {value}")
