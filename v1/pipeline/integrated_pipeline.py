"""
Integrated scraping + processing pipeline.

Scrapes REAL company pages in batches (default 300), saves to SQLite, then runs the Rust processor.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from Python_Scraping.yc_scraper import (
    get_conn,
    mark_visited,
    save_pagedata,
    scrape_batch,
)

BATCH_SIZE = 300
PROCESSOR_PATH = ROOT / "Rust_Processing"


def get_real_company_urls(limit: int = 100):
    """
    Get unvisited REAL company URLs (not category/duplicate pages).
    Filters out:
    - /industry/, /location/, /batch/, /tags/ pages
    - job and launch pages
    - duplicates

    Magic number: ~5,571 real companies total.
    """
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT DISTINCT id, url FROM websites_from_sitemap 
        WHERE visited = 0
        AND url LIKE 'https://www.ycombinator.com/companies/%'
        AND url NOT LIKE '%/industry/%'
        AND url NOT LIKE '%/location/%'
        AND url NOT LIKE '%/batch/%'
        AND url NOT LIKE '%/tags/%'
        AND url NOT LIKE '%/jobs%'
        AND url NOT LIKE '%/launches%'
        LIMIT ?
        """,
        (limit,),
    )

    results = [(row["id"], row["url"]) for row in cursor.fetchall()]
    conn.close()
    return results


def count_real_companies(visited: bool = False):
    """Count REAL company pages (not categories, no duplicates)."""
    conn = get_conn()
    cursor = conn.cursor()

    visited_clause = "visited = 1" if visited else "visited = 0"

    cursor.execute(
        f"""
        SELECT COUNT(DISTINCT url) FROM websites_from_sitemap 
        WHERE {visited_clause}
        AND url LIKE 'https://www.ycombinator.com/companies/%'
        AND url NOT LIKE '%/industry/%'
        AND url NOT LIKE '%/location/%'
        AND url NOT LIKE '%/batch/%'
        AND url NOT LIKE '%/tags/%'
        AND url NOT LIKE '%/jobs%'
        AND url NOT LIKE '%/launches%'
        """
    )

    count = cursor.fetchone()[0]
    conn.close()
    return count


def run_processor():
    """Run the Rust processor on the current database."""
    print("\n🔧 Running processor on batch...")

    # Use pre-built binary instead of cargo run
    processor_bin = PROCESSOR_PATH / "target" / "release" / "yc_processor_v1"

    if not processor_bin.exists():
        print(f"❌ Processor binary not found at {processor_bin}")
        print("   Run: cd processing_tech && cargo build --release")
        return False

    try:
        import os

        env = os.environ.copy()
        env["YC_DB_PATH"] = str(ROOT / "Sqlite_Database" / "data" / "yc.sqlite")

        result = subprocess.run(
            [str(processor_bin)],
            cwd=PROCESSOR_PATH,
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )

        if result.returncode != 0:
            print(f"❌ Processor failed: {result.stderr}")
            return False

        # Print key stats from output
        for line in result.stdout.split("\n"):
            if (
                "->" in line
                or "Stats" in line
                or any(
                    x in line for x in ["companies:", "founders:", "links:", "news:"]
                )
            ):
                print(f"  {line}")

        print("✓ Processor completed\n")
        return True
    except subprocess.TimeoutExpired:
        print("❌ Processor timed out")
        return False
    except Exception as e:
        print(f"❌ Processor error: {e}")
        return False


def run_integrated_pipeline():
    """
    Run the integrated scraping + processing pipeline.

    Flow:
    1. Scrape batch N (200 REAL companies, no duplicates/categories)
    2. Save to database
    3. Run processor on batch N
    4. Repeat until all ~5,571 companies done
    """

    # Count total unvisited REAL companies (magic number: ~5,571)
    total = count_real_companies(visited=False)
    print(f"🚀 Starting integrated pipeline")
    print(f"📊 Total REAL companies to process: {total}")
    print(f"   Expected: ~5,571 (below 6,000)\n")

    if total == 0:
        print("No companies to scrape!")
        return

    if total > 6000:
        print(f"⚠️  WARNING: Found {total} companies, expected <6,000!")
        print("   Check filters - may include category pages!")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != "y":
            return

    batch_num = 0
    total_scraped = 0
    total_processed = 0

    while True:
        batch_num += 1
        remaining = count_real_companies(visited=False)

        if remaining == 0:
            print("✅ All companies processed!")
            break

        print(f"{'=' * 60}")
        print(f"📦 BATCH {batch_num} - {remaining} companies remaining")
        print(f"{'=' * 60}")

        # Step 1: Get REAL company URLs (filtered, no duplicates)
        url_data = get_real_company_urls(limit=BATCH_SIZE)
        if not url_data:
            print("No URLs found (shouldn't happen)")
            break

        ids = [x[0] for x in url_data]
        urls = [x[1] for x in url_data]
        id_map = {url: sid for sid, url in url_data}

        print(f"🌐 Scraping {len(urls)} companies via Desync...")

        # Step 2: Scrape
        try:
            pages = scrape_batch(urls)
            print(f"  ✓ Got {len(pages)} results from Desync")
        except Exception as e:
            print(f"  ❌ Scraping failed: {e}")
            mark_visited(ids)
            continue

        # Step 3: Save to database
        saved = save_pagedata(pages, id_map)
        mark_visited(ids)
        total_scraped += saved
        print(f"  ✓ Saved {saved} pages to database")
        print(f"  ✓ Marked {len(ids)} URLs as visited")

        # Step 4: Run processor on this batch
        if run_processor():
            total_processed += saved

        print(
            f"\n📈 Progress: {total_scraped}/{total} scraped, {total_processed} processed\n"
        )

    print(f"\n{'=' * 60}")
    print(f"✅ PIPELINE COMPLETE")
    print(f"{'=' * 60}")
    print(f"Total scraped: {total_scraped}")
    print(f"Total processed: {total_processed}")


if __name__ == "__main__":
    try:
        run_integrated_pipeline()
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
