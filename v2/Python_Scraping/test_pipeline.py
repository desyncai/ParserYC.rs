"""Lightweight smoke test for the integrated pipeline (10 companies)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from pipeline.integrated_pipeline import run_processor
from Python_Scraping.pipeline.coordinator import ScrapeCoordinator

coord = ScrapeCoordinator(batch_size=10)

print("🧪 Testing pipeline with 10 companies...\n")

total = coord.get_real_company_count(visited=False)
print(f"Total companies available: {total}")

if total < 10:
    print("Not enough companies!")
    sys.exit(1)

result = coord.run_batch(use_real_company_filter=True)

if result.saved == 0:
    print("Scrape returned no pages; aborting test.")
    sys.exit(1)

print("\n⚙️  Processing scraped batch...")
success = run_processor()

if success:
    print("\n✅ Test pipeline PASSED!")
else:
    print("\n❌ Test pipeline FAILED!")
    sys.exit(1)
