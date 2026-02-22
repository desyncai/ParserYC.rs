"""
Back-compat entrypoint for jobs scraping. Delegates to Python_Scraping.jobs.* modules.
"""

from __future__ import annotations

import argparse
from typing import Optional

from Python_Scraping.jobs.loader import init_jobs_page_data
from Python_Scraping.jobs.pipeline import (
    compare_with_jobs_sitemap,
    process_jobs_batch,
    run_jobs_pipeline,
)


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
        default=300,
        help="Batch size (default: 300)",
    )
    parser.add_argument(
        "--wait-time",
        type=int,
        help="Override the default Desync wait_time for this job pipeline",
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
    else:
        if args.init:
            inserted = init_jobs_page_data(drop_existing=True)
            print(f"Seeded {inserted} job URLs into jobs_page_data.")

        if args.compare_sitemap:
            compare_with_jobs_sitemap()

        if args.pipeline:
            run_jobs_pipeline(batch_size=args.batch_size, wait_time=args.wait_time)
        elif args.batch:
            process_jobs_batch(batch_size=args.batch_size, wait_time=args.wait_time)
