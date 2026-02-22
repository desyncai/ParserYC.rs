"""
Integrated scraping + processing pipeline.
Scrapes REAL company pages in batches, saves to SQLite, then runs the Rust processor.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from Python_Scraping.pipeline.coordinator import ScrapeCoordinator
from Sqlite_Database import schema as db_schema

ROOT = Path(__file__).resolve().parent.parent
PROCESSOR_PATH = ROOT / "Rust_Processing"
PROCESSOR_BIN = PROCESSOR_PATH / "target" / "release" / "company_metadata_extraction"
BATCH_SIZE = 300


def run_processor() -> bool:
    """Run the Rust processor on the current database."""
    print("\n🔧 Running processor on batch...")

    if not PROCESSOR_BIN.exists():
        print(f"❌ Processor binary not found at {PROCESSOR_BIN}")
        print(
            "   Run: cd Rust_Processing && cargo build --release --bin company_metadata_extraction"
        )
        return False

    env = os.environ.copy()
    env["YC_DB_PATH"] = str(db_schema.DB_PATH)

    try:
        result = subprocess.run(
            [str(PROCESSOR_BIN)],
            cwd=PROCESSOR_PATH,
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )
    except subprocess.TimeoutExpired:
        print("❌ Processor timed out")
        return False
    except Exception as exc:
        print(f"❌ Processor error: {exc}")
        return False

    if result.returncode != 0:
        print(f"❌ Processor failed: {result.stderr}")
        return False

    for line in result.stdout.split("\n"):
        if "chars after pass" in line or "Stats" in line or "companies:" in line:
            print(f"  {line}")

    print("✓ Processor completed\n")
    return True


def run_integrated_pipeline(
    batch_size: int = BATCH_SIZE, wait_time: int | None = None
) -> None:
    coord = ScrapeCoordinator(batch_size=batch_size, wait_time=wait_time)

    total = coord.get_real_company_count(visited=False)
    print(" Starting integrated pipeline")
    print(f" Total REAL companies to process: {total}")
    print("   Expected: ~5,571 (below 6,000)\n")

    if total == 0:
        print("No companies to scrape!")
        return

    if total > 6000:
        print(f"  WARNING: Found {total} companies, expected <6,000!")
        print("   Check filters - may include category pages!")

    batch_num = 0
    total_scraped = 0
    total_processed = 0

    while True:
        batch_num += 1
        remaining = coord.get_real_company_count(visited=False)

        if remaining == 0:
            print(" All companies processed!")
            break

        print(f"{'=' * 60}")
        print(f" BATCH {batch_num} - {remaining} companies remaining")
        print(f"{'=' * 60}")

        result = coord.run_batch(use_real_company_filter=True)
        if result.attempted == 0:
            print("No URLs found (shouldn't happen)")
            break

        total_scraped += result.saved

        if run_processor():
            total_processed += result.saved

        print(
            f"\n📈 Progress: {total_scraped}/{total} scraped, {total_processed} processed\n"
        )

    print(f"\n{'=' * 60}")
    print(" PIPELINE COMPLETE")
    print(f"{'=' * 60}")
    print(f"Total scraped: {total_scraped}")
    print(f"Total processed: {total_processed}")


if __name__ == "__main__":
    run_integrated_pipeline()
