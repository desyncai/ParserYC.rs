"""
Back-compat wrappers that delegate to the modular pipeline.
"""

from __future__ import annotations

import argparse
from typing import Optional

from Python_Scraping.pipeline.coordinator import BatchResult, ScrapeCoordinator

default_coordinator = ScrapeCoordinator()


# --- Back-compat free functions (thin wrappers over the coordinator) ---


def scrape_batch(urls: list[str], wait_time: Optional[int] = None):
    return default_coordinator.scraper.bulk_search(urls, wait_time=wait_time).pages


def save_pagedata(pages, id_map: dict[str, int]) -> int:
    return default_coordinator.store.save_pagedata(pages, id_map)


def mark_visited(ids: list[int]):
    return default_coordinator.store.mark_visited(ids)


def get_real_company_urls(limit: int = 100):
    return default_coordinator._fetch_real_company_urls(limit)


def count_real_companies(visited: bool = False):
    return default_coordinator.get_real_company_count(visited=visited)


def run_pipeline(
    pattern: Optional[str] = None,
    exclude: Optional[str] = None,
    batch_size: int = 100,
    max_batches: Optional[int] = None,
    use_real_company_filter: bool = False,
):
    coord = ScrapeCoordinator(
        store=default_coordinator.store,
        batch_size=batch_size,
        scraper=default_coordinator.scraper,
    )
    return coord.run_pipeline(
        pattern=pattern,
        exclude=exclude,
        max_batches=max_batches,
        use_real_company_filter=use_real_company_filter,
    )


def stats():
    return default_coordinator.stats()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YC Scraper")
    parser.add_argument("--pattern", type=str, help="URL filter (e.g., /companies/)")
    parser.add_argument(
        "--exclude", type=str, help="URL substring to exclude (e.g., /jobs/)"
    )
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-batches", type=int, help="Max batches to run")
    parser.add_argument(
        "--real",
        action="store_true",
        help="Use the strict real-company filter (excludes jobs/launches/tags/etc.)",
    )
    parser.add_argument("--stats", action="store_true", help="Show stats only")
    args = parser.parse_args()

    if args.stats:
        stats()
    else:
        coord = ScrapeCoordinator(batch_size=args.batch_size)
        coord.run_pipeline(
            pattern=args.pattern,
            exclude=args.exclude,
            max_batches=args.max_batches,
            use_real_company_filter=args.real,
        )
