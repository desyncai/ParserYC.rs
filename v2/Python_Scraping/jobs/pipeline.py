from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, Optional

from Python_Scraping.core.desync_client import DesyncScraper
from Python_Scraping.core.logging import console
from Python_Scraping.core.store import SQLiteStore, default_store
from Python_Scraping.jobs.loader import (
    init_jobs_page_data,
    jobs_table_exists,
    mark_job_urls_visited,
)

ROOT = Path(__file__).resolve().parents[2]
JOBS_SITEMAP_PATH = ROOT / "XML_Sitemaps" / "jobs_sitemap.xml"
BATCH_SIZE_DEFAULT = 300


def process_jobs_batch(
    *,
    batch_size: int = BATCH_SIZE_DEFAULT,
    wait_time: Optional[int] = None,
    store: SQLiteStore = default_store,
    scraper: Optional[DesyncScraper] = None,
) -> int:
    scraper = scraper or DesyncScraper()
    rows = store.fetch_job_batch(limit=batch_size)
    if not rows:
        console.print("[yellow]No unvisited job URLs left.[/]")
        return 0

    job_ids = [row[0] for row in rows]
    urls = [row[1] for row in rows]
    id_map = {row[1]: row[0] for row in rows}

    console.print(f"[cyan]\n--- Scraping {len(urls)} job URLs ---[/]")
    bulk = scraper.bulk_search(urls, wait_time=wait_time)
    console.print(f"  Got {len(bulk.pages)} results")
    saved = store.save_job_pages(bulk.pages, id_map)
    console.print(f"  Saved {saved} job pages")

    mark_job_urls_visited(job_ids, store=store)
    console.print(f"  Marked {len(job_ids)} job URLs as visited")

    empty = len(urls) - len(bulk.pages)
    if empty:
        console.print(f"  Empty/missing results this batch: {empty}")

    return saved


def count_jobs(
    *, visited: bool | None = None, store: SQLiteStore = default_store
) -> int:
    return store.count_jobs(visited=visited)


def run_jobs_pipeline(
    *,
    batch_size: int = BATCH_SIZE_DEFAULT,
    wait_time: Optional[int] = None,
    store: SQLiteStore = default_store,
) -> None:
    batch_num = 0
    total_saved = 0

    while True:
        remaining = count_jobs(visited=False, store=store)
        if remaining == 0:
            console.print("\n[green]✅ All job URLs processed.[/]")
            break

        batch_num += 1
        console.rule(f"Batch {batch_num}: {remaining} remaining")
        saved = process_jobs_batch(
            batch_size=batch_size, wait_time=wait_time, store=store
        )
        total_saved += saved

        if saved == 0:
            console.print("No pages saved this batch; stopping early to avoid a loop.")
            break

    console.print(f"\nDone. Total job pages saved: {total_saved}")


def compare_with_jobs_sitemap(
    sitemap_path: Path = JOBS_SITEMAP_PATH, store: SQLiteStore = default_store
) -> Dict[str, int]:
    if not sitemap_path.exists():
        console.print(f"[red]Sitemap not found at {sitemap_path}")
        return {}

    tree = ET.parse(sitemap_path)
    sitemap_urls = {
        el.text.strip()
        for el in tree.iterfind(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        if el.text
    }

    if not jobs_table_exists(store=store):
        console.print("[yellow]jobs_page_data not found. Run init first.")
        return {}

    with store.connect() as conn:
        cur = conn.cursor()
        table_urls = {
            row["url"]
            for row in cur.execute("SELECT url FROM jobs_page_data").fetchall()
        }

    overlap = len(table_urls & sitemap_urls)
    only_in_table = len(table_urls - sitemap_urls)
    only_in_sitemap = len(sitemap_urls - table_urls)

    console.print("Jobs sitemap overlap (no DB writes):")
    console.print(f"  In DB: {len(table_urls)}")
    console.print(f"  In sitemap: {len(sitemap_urls)}")
    console.print(f"  Overlap: {overlap}")
    console.print(f"  DB only: {only_in_table}")
    console.print(f"  Sitemap only: {only_in_sitemap}")

    return {
        "db": len(table_urls),
        "sitemap": len(sitemap_urls),
        "overlap": overlap,
        "db_only": only_in_table,
        "sitemap_only": only_in_sitemap,
    }
