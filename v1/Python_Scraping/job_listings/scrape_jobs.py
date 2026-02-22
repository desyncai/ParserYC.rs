"""
Jobs scraper using the shared scraping helpers in Python_Scraping.yc_scraper.

Run these three functions in sequence:
1) init_jobs_page_data()  - drop/create jobs_page_data and seed it with job URLs (visited=0)
2) process_jobs_batch()   - run one bulk scrape of up to 300 URLs and mark them visited
3) run_jobs_pipeline()    - keep running batches until everything is visited

Optional: compare_with_jobs_sitemap() reports overlap with data/jobs_sitemap.xml without mutating state.
"""

import argparse
import json
import sqlite3
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from Python_Scraping.yc_scraper import get_conn, scrape_batch

JOBS_SITEMAP_PATH = ROOT / "XML_Sitemaps" / "jobs_sitemap.xml"
BATCH_SIZE_DEFAULT = 300


def init_jobs_page_data(drop_existing: bool = True) -> int:
    """
    Create jobs_page_data and seed it with job URLs from websites_from_sitemap.

    Returns:
        Number of URLs inserted (ignores duplicates).
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        if drop_existing:
            cur.execute("DROP TABLE IF EXISTS jobs_page_data")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs_page_data (
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                visited INTEGER DEFAULT 0,
                visited_at TEXT,
                sitemap_id INTEGER,
                desync_id INTEGER,
                domain TEXT,
                timestamp INTEGER,
                bulk_search_id TEXT,
                search_type TEXT,
                text_content TEXT,
                html_content TEXT,
                internal_links TEXT,
                external_links TEXT,
                latency_ms INTEGER,
                complete INTEGER,
                created_at INTEGER,
                scraped_at TEXT,
                FOREIGN KEY (sitemap_id) REFERENCES websites_from_sitemap(id)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_url ON jobs_page_data(url)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_visited ON jobs_page_data(visited)"
        )

        before = cur.execute("SELECT COUNT(*) FROM jobs_page_data").fetchone()[0]
        rows = cur.execute(
            """
            SELECT id, url
            FROM websites_from_sitemap
            WHERE url LIKE 'https://www.ycombinator.com/companies/%/jobs/%'
            """
        ).fetchall()

        payload = [(row["url"], row["id"]) for row in rows]
        if payload:
            cur.executemany(
                """
                INSERT OR IGNORE INTO jobs_page_data (url, sitemap_id, visited)
                VALUES (?, ?, 0)
                """,
                payload,
            )

        after = cur.execute("SELECT COUNT(*) FROM jobs_page_data").fetchone()[0]
        conn.commit()
        return after - before
    finally:
        conn.close()


def jobs_table_exists(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    row = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs_page_data'"
    ).fetchone()
    return bool(row)


def mark_job_urls_visited(conn: sqlite3.Connection, job_ids: Iterable[int]) -> None:
    """Mark jobs as visited after they have been sent to simple bulk."""
    ids = list(job_ids)
    if not ids:
        return
    cur = conn.cursor()
    cur.executemany(
        """
        UPDATE jobs_page_data
        SET visited = 1, visited_at = CURRENT_TIMESTAMP
        WHERE job_id = ?
        """,
        [(i,) for i in ids],
    )
    conn.commit()


def save_job_pages(conn: sqlite3.Connection, pages, id_map: Dict[str, int]) -> int:
    """Persist scraped job pages back into jobs_page_data."""
    if not pages:
        return 0

    cur = conn.cursor()
    saved = 0
    for p in pages:
        job_id = id_map.get(p.url)
        if job_id is None:
            continue
        cur.execute(
            """
            UPDATE jobs_page_data
            SET desync_id = ?, domain = ?, timestamp = ?, bulk_search_id = ?,
                search_type = ?, text_content = ?, html_content = ?,
                internal_links = ?, external_links = ?, latency_ms = ?,
                complete = ?, created_at = ?, scraped_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
            """,
            (
                p.id,
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
                job_id,
            ),
        )
        saved += 1

    conn.commit()
    return saved


def process_jobs_batch(batch_size: int = BATCH_SIZE_DEFAULT) -> int:
    """
    Scrape a single batch of unvisited job URLs (visited flips once sent to bulk).

    Returns:
        Number of pages saved.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT job_id, url
            FROM jobs_page_data
            WHERE visited = 0
            LIMIT ?
            """,
            (batch_size,),
        ).fetchall()

        if not rows:
            print("No unvisited job URLs left.")
            return 0

        job_ids = [row["job_id"] for row in rows]
        urls = [row["url"] for row in rows]
        id_map = {row["url"]: row["job_id"] for row in rows}

        print(f"\n--- Scraping {len(urls)} job URLs ---")
        try:
            pages = scrape_batch(urls)
        except Exception as e:
            print(f"Bulk search failed: {e}")
            return 0

        print(f"  Got {len(pages)} results")
        saved = save_job_pages(conn, pages, id_map)
        print(f"  Saved {saved} job pages")

        mark_job_urls_visited(conn, job_ids)
        print(f"  Marked {len(job_ids)} job URLs as visited")

        empty = len(urls) - len(pages)
        if empty:
            print(f"  Empty/missing results this batch: {empty}")

        return saved
    finally:
        conn.close()


def count_jobs(visited: bool | None = None) -> int:
    """Count jobs by visited state (or all if visited is None)."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        if visited is None:
            row = cur.execute("SELECT COUNT(*) FROM jobs_page_data").fetchone()
        else:
            row = cur.execute(
                "SELECT COUNT(*) FROM jobs_page_data WHERE visited = ?",
                (1 if visited else 0,),
            ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def run_jobs_pipeline(batch_size: int = BATCH_SIZE_DEFAULT) -> None:
    """Keep scraping batches sequentially until every job URL is visited."""
    batch_num = 0
    total_saved = 0

    while True:
        remaining = count_jobs(visited=False)
        if remaining == 0:
            print("\n✅ All job URLs processed.")
            break

        batch_num += 1
        print(f"\n=== Batch {batch_num}: {remaining} remaining ===")
        saved = process_jobs_batch(batch_size=batch_size)
        total_saved += saved

        if saved == 0:
            print("No pages saved this batch; stopping early to avoid a loop.")
            break

    print(f"\nDone. Total job pages saved: {total_saved}")


def compare_with_jobs_sitemap() -> Dict[str, int]:
    """
    Compare jobs_page_data URLs with data/jobs_sitemap.xml.
    Does not mutate the database.
    """
    if not JOBS_SITEMAP_PATH.exists():
        print(f"Sitemap not found at {JOBS_SITEMAP_PATH}")
        return {}

    tree = ET.parse(JOBS_SITEMAP_PATH)
    sitemap_urls = {
        el.text.strip()
        for el in tree.iterfind(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        if el.text
    }

    conn = get_conn()
    try:
        if not jobs_table_exists(conn):
            print("jobs_page_data not found. Run --init first.")
            return {}

        cur = conn.cursor()
        table_urls = {
            row["url"]
            for row in cur.execute("SELECT url FROM jobs_page_data").fetchall()
        }
    finally:
        conn.close()

    overlap = len(table_urls & sitemap_urls)
    only_in_table = len(table_urls - sitemap_urls)
    only_in_sitemap = len(sitemap_urls - table_urls)

    print("Jobs sitemap overlap (no DB writes):")
    print(f"  In DB: {len(table_urls)}")
    print(f"  In sitemap: {len(sitemap_urls)}")
    print(f"  Overlap: {overlap}")
    print(f"  DB only: {only_in_table}")
    print(f"  Sitemap only: {only_in_sitemap}")

    return {
        "db": len(table_urls),
        "sitemap": len(sitemap_urls),
        "overlap": overlap,
        "db_only": only_in_table,
        "sitemap_only": only_in_sitemap,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape YC job listings into jobs_page_data"
    )
    parser.add_argument(
        "--init", action="store_true", help="Recreate and seed jobs_page_data"
    )
    parser.add_argument(
        "--batch", action="store_true", help="Run a single batch (requires prior init)"
    )
    parser.add_argument(
        "--pipeline",
        action="store_true",
        help="Run the full pipeline until all job URLs are visited",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE_DEFAULT,
        help="Batch size (default: 300)",
    )
    parser.add_argument(
        "--compare-sitemap",
        action="store_true",
        help="Print overlap stats versus data/jobs_sitemap.xml (no scraping)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if not any([args.init, args.batch, args.pipeline, args.compare_sitemap]):
        print(
            "No action provided. Use --init, --batch, --pipeline, or --compare-sitemap."
        )
        sys.exit(0)

    if args.init:
        inserted = init_jobs_page_data(drop_existing=True)
        print(f"Seeded {inserted} job URLs into jobs_page_data.")

    if args.compare_sitemap:
        compare_with_jobs_sitemap()

    if args.pipeline:
        run_jobs_pipeline(batch_size=args.batch_size)
    elif args.batch:
        process_jobs_batch(batch_size=args.batch_size)
