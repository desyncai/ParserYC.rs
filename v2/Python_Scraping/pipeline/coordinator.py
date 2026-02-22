from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from Python_Scraping.core.desync_client import DesyncScraper
from Python_Scraping.core.filters import REAL_COMPANY_FILTERS
from Python_Scraping.core.logging import console, info
from Python_Scraping.core.store import SQLiteStore, default_store


@dataclass
class BatchResult:
    attempted: int
    saved: int
    visited: int
    error: Optional[Exception] = None


class ScrapeCoordinator:
    """Orchestrates URL selection -> Desync scrape -> DB persistence."""

    def __init__(
        self,
        *,
        store: SQLiteStore = default_store,
        scraper: Optional[DesyncScraper] = None,
        batch_size: int = 100,
        wait_time: Optional[int] = None,
    ) -> None:
        self.store = store
        self.scraper = scraper or DesyncScraper()
        self.batch_size = batch_size
        self.wait_time = wait_time

    # --- URL selection helpers ---
    def _fetch_real_company_urls(self, limit: int) -> list[tuple[int, str]]:
        records = self.store.fetch_urls(limit=limit, extra_clauses=REAL_COMPANY_FILTERS)
        return [(r.id, r.url) for r in records]

    def get_real_company_count(self, *, visited: bool = False) -> int:
        return self.store.count_urls(
            visited=visited, extra_clauses=REAL_COMPANY_FILTERS
        )

    # --- batch + pipeline runners ---
    def run_batch(
        self,
        *,
        pattern: Optional[str] = None,
        exclude: Optional[str] = None,
        use_real_company_filter: bool = False,
    ) -> BatchResult:
        if use_real_company_filter:
            url_data = self._fetch_real_company_urls(self.batch_size)
        else:
            url_records = self.store.fetch_urls(
                pattern=pattern,
                exclude=exclude,
                limit=self.batch_size,
            )
            url_data = [(r.id, r.url) for r in url_records]

        if not url_data:
            console.print("[bold yellow]No unvisited URLs found.[/]")
            return BatchResult(attempted=0, saved=0, visited=0)

        ids = [row[0] for row in url_data]
        urls = [row[1] for row in url_data]
        id_map = {url: sid for sid, url in url_data}

        console.print(f"[cyan]Scraping {len(urls)} URLs via Desync...[/]")
        bulk = self.scraper.bulk_search(urls, wait_time=self.wait_time)
        console.print(
            f"  Got {len(bulk.pages)} results (attempted {bulk.attempted}, wait={bulk.wait_time}, html={bulk.extract_html})"
        )

        saved = self.store.save_pagedata(bulk.pages, id_map)
        console.print(f"  Saved {saved} pages")

        self.store.mark_visited(ids)
        console.print(f"  Marked {len(ids)} URLs as visited")

        return BatchResult(
            attempted=len(urls), saved=saved, visited=len(ids), error=bulk.error
        )

    def run_pipeline(
        self,
        *,
        pattern: Optional[str] = None,
        exclude: Optional[str] = None,
        max_batches: Optional[int] = None,
        use_real_company_filter: bool = False,
    ) -> int:
        total = (
            self.get_real_company_count(visited=False)
            if use_real_company_filter
            else self.store.count_urls(pattern=pattern, exclude=exclude, visited=False)
        )
        desc = (
            "real company pages"
            if use_real_company_filter
            else f"matching '{pattern}'"
            if pattern
            else "all unvisited"
        )
        if exclude:
            desc += f" excluding '{exclude}'"
        info(f"Starting pipeline: {total} {desc}")

        batch_num = 0
        total_scraped = 0

        while True:
            batch_num += 1
            if max_batches and batch_num > max_batches:
                console.print(f"[yellow]Reached max batches ({max_batches})[/]")
                break

            remaining = (
                self.get_real_company_count(visited=False)
                if use_real_company_filter
                else self.store.count_urls(
                    pattern=pattern, exclude=exclude, visited=False
                )
            )
            if remaining == 0:
                console.print("[green]All URLs processed![/]")
                break

            console.rule(f"Batch {batch_num} ({remaining} remaining)")
            result = self.run_batch(
                pattern=pattern,
                exclude=exclude,
                use_real_company_filter=use_real_company_filter,
            )
            total_scraped += result.saved
            if result.attempted == 0:
                break

        info(f"Pipeline complete. Total scraped: {total_scraped}")
        return total_scraped

    # --- stats ---
    def stats(self) -> dict:
        with self.store.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM websites_from_sitemap")
            total = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(*) FROM websites_from_sitemap WHERE visited = 0"
            )
            unvisited = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(*) FROM websites_from_sitemap WHERE visited = 1"
            )
            visited = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT sitemap_source,
                       SUM(CASE WHEN visited = 0 THEN 1 ELSE 0 END) as unvisited,
                       SUM(CASE WHEN visited = 1 THEN 1 ELSE 0 END) as visited
                FROM websites_from_sitemap GROUP BY sitemap_source
            """
            )
            by_source = {
                row["sitemap_source"]: {
                    "unvisited": row["unvisited"],
                    "visited": row["visited"],
                }
                for row in cursor.fetchall()
            }

            cursor.execute("SELECT COUNT(*) FROM pagedataobjects")
            total_pagedata = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM pagedataobjects WHERE complete = 1")
            complete_pagedata = cursor.fetchone()[0]

        stats = {
            "urls": {
                "total": total,
                "unvisited": unvisited,
                "visited": visited,
                "by_source": by_source,
            },
            "pagedata": {"total": total_pagedata, "complete": complete_pagedata},
        }
        console.print(stats)
        return stats
