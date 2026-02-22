"""
YC Website Scraper using Desync AI API.
Pipeline: get unvisited URLs -> bulk search -> save results -> mark visited -> repeat
"""

import json
import os
import sqlite3
from pathlib import Path
from typing import Optional

_client = None
# Database now lives in Sqlite_Database/data/yc.sqlite at the project root.
DB_PATH = (
    Path(__file__).resolve().parent.parent / "Sqlite_Database" / "data" / "yc.sqlite"
)


def get_client():
    """Get or create Desync client."""
    global _client
    if _client is None:
        from desync_search import DesyncClient

        api_key = os.getenv("DESYNC_API_KEY")
        if not api_key:
            raise ValueError("Set DESYNC_API_KEY environment variable")
        _client = DesyncClient(user_api_key=api_key)
    return _client


def get_conn() -> sqlite3.Connection:
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# --- URL Management ---


def get_unvisited_urls(
    pattern: Optional[str] = None, exclude: Optional[str] = None, limit: int = 100
) -> list[tuple[int, str]]:
    """
    Get unvisited URLs from sitemap table.

    Args:
        pattern: URL substring filter (e.g., '/companies/')
        exclude: URL substring to exclude (e.g., '/jobs/')
        limit: Max URLs to return

    Returns:
        List of (id, url) tuples
    """
    conn = get_conn()
    cursor = conn.cursor()

    conditions = ["visited = 0"]
    params = []

    if pattern:
        conditions.append("url LIKE ?")
        params.append(f"%{pattern}%")
    if exclude:
        conditions.append("url NOT LIKE ?")
        params.append(f"%{exclude}%")

    params.append(limit)
    where = " AND ".join(conditions)
    cursor.execute(
        f"SELECT id, url FROM websites_from_sitemap WHERE {where} LIMIT ?", params
    )

    results = [(row["id"], row["url"]) for row in cursor.fetchall()]
    conn.close()
    return results


def mark_visited(ids: list[int]):
    """Mark URLs as visited by their IDs."""
    if not ids:
        return
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executemany(
        """
        UPDATE websites_from_sitemap
        SET visited = 1, visited_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """,
        [(i,) for i in ids],
    )
    conn.commit()
    conn.close()


def count_urls(
    pattern: Optional[str] = None,
    exclude: Optional[str] = None,
    visited: Optional[bool] = None,
) -> int:
    """Count URLs matching criteria."""
    conn = get_conn()
    cursor = conn.cursor()

    conditions = []
    params = []

    if pattern:
        conditions.append("url LIKE ?")
        params.append(f"%{pattern}%")
    if exclude:
        conditions.append("url NOT LIKE ?")
        params.append(f"%{exclude}%")
    if visited is not None:
        conditions.append("visited = ?")
        params.append(1 if visited else 0)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    cursor.execute(f"SELECT COUNT(*) FROM websites_from_sitemap {where}", params)
    count = cursor.fetchone()[0]
    conn.close()
    return count


# --- PageData Storage ---


def save_pagedata(pages, id_map: dict[str, int]) -> int:
    """
    Save PageData objects to database.

    Args:
        pages: List of PageData objects from Desync
        id_map: Mapping of URL -> sitemap_id for FK relationship

    Returns:
        Number of pages saved
    """
    if not pages:
        return 0

    conn = get_conn()
    cursor = conn.cursor()
    saved = 0

    for p in pages:
        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO pagedataobjects (
                    id, url, domain, timestamp, bulk_search_id, search_type,
                    text_content, html_content, internal_links, external_links,
                    latency_ms, complete, created_at, sitemap_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    p.id,
                    p.url,
                    p.domain,
                    p.timestamp,
                    p.bulk_search_id,
                    p.search_type,
                    p.text_content,
                    getattr(p, "html_content", None),
                    json.dumps(p.internal_links) if p.internal_links else None,
                    json.dumps(p.external_links) if p.external_links else None,
                    p.latency_ms,
                    1 if p.complete else 0,
                    p.created_at,
                    id_map.get(p.url),
                ),
            )
            saved += 1
        except Exception as e:
            print(f"  Error saving {p.url}: {e}")

    conn.commit()
    conn.close()
    return saved


# --- Scraping ---


def scrape_batch(urls: list[str], wait_time: Optional[int] = None):
    """
    Scrape URLs using Desync bulk search, letting Desync decide how long to hold the
    request open unless a custom wait_time is provided.
    """
    client = get_client()
    payload = {"target_list": urls, "extract_html": False}
    if wait_time is not None:
        payload["wait_time"] = wait_time
    return client.simple_bulk_search(**payload)


# --- Pipeline ---


def run_batch(
    pattern: Optional[str] = None,
    exclude: Optional[str] = None,
    batch_size: int = 100,
    wait_time: Optional[int] = None,
) -> int:
    """
    Run one batch: get URLs -> scrape -> save -> mark visited.

    Returns:
        Number of pages scraped
    """
    # 1. Get unvisited URLs
    url_data = get_unvisited_urls(pattern=pattern, exclude=exclude, limit=batch_size)
    if not url_data:
        print("No unvisited URLs found.")
        return 0

    ids = [x[0] for x in url_data]
    urls = [x[1] for x in url_data]
    id_map = {url: sid for sid, url in url_data}

    print(f"Scraping {len(urls)} URLs...")

    # 2. Bulk search
    try:
        pages = scrape_batch(urls, wait_time=wait_time)
    except Exception as e:
        print(f"Bulk search failed: {e}")
        return 0

    print(f"  Got {len(pages)} results")

    # 3. Save to DB
    saved = save_pagedata(pages, id_map)
    print(f"  Saved {saved} pages")

    # 4. Mark visited (all attempted URLs, not just successful)
    mark_visited(ids)
    print(f"  Marked {len(ids)} URLs as visited")

    return saved


def run_pipeline(
    pattern: Optional[str] = None,
    exclude: Optional[str] = None,
    batch_size: int = 100,
    max_batches: Optional[int] = None,
):
    """
    Run full scraping pipeline until no URLs left or max_batches reached.
    """
    total = count_urls(pattern=pattern, exclude=exclude, visited=False)
    desc = f"matching '{pattern}'" if pattern else ""
    if exclude:
        desc += f" excluding '{exclude}'"
    print(f"Starting pipeline: {total} unvisited URLs {desc}")

    batch_num = 0
    total_scraped = 0

    while True:
        batch_num += 1
        if max_batches and batch_num > max_batches:
            print(f"Reached max batches ({max_batches})")
            break

        remaining = count_urls(pattern=pattern, exclude=exclude, visited=False)
        if remaining == 0:
            print("All URLs processed!")
            break

        print(f"\n--- Batch {batch_num} ({remaining} remaining) ---")
        scraped = run_batch(pattern=pattern, exclude=exclude, batch_size=batch_size)
        total_scraped += scraped

    print(f"\nPipeline complete. Total scraped: {total_scraped}")
    return total_scraped


def stats():
    """Print current stats."""
    conn = get_conn()
    cursor = conn.cursor()

    print("=== Sitemap URLs ===")
    cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap")
    print(f"Total: {cursor.fetchone()[0]}")

    cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap WHERE visited = 0")
    print(f"Unvisited: {cursor.fetchone()[0]}")

    cursor.execute("""
        SELECT sitemap_source,
               SUM(CASE WHEN visited = 0 THEN 1 ELSE 0 END) as unvisited,
               SUM(CASE WHEN visited = 1 THEN 1 ELSE 0 END) as visited
        FROM websites_from_sitemap GROUP BY sitemap_source
    """)
    for row in cursor.fetchall():
        print(
            f"  {row['sitemap_source']}: {row['unvisited']} unvisited, {row['visited']} visited"
        )

    print("\n=== PageData ===")
    cursor.execute("SELECT COUNT(*) FROM pagedataobjects")
    print(f"Total scraped: {cursor.fetchone()[0]}")

    cursor.execute("SELECT COUNT(*) FROM pagedataobjects WHERE complete = 1")
    print(f"Complete: {cursor.fetchone()[0]}")

    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="YC Scraper")
    parser.add_argument("--pattern", type=str, help="URL filter (e.g., /companies/)")
    parser.add_argument(
        "--exclude", type=str, help="URL substring to exclude (e.g., /jobs/)"
    )
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-batches", type=int, help="Max batches to run")
    parser.add_argument("--stats", action="store_true", help="Show stats only")
    args = parser.parse_args()

    if args.stats:
        stats()
    else:
        run_pipeline(
            pattern=args.pattern,
            exclude=args.exclude,
            batch_size=args.batch_size,
            max_batches=args.max_batches,
        )
