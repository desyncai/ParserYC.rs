"""Test the pipeline with just 10 companies first (lightweight smoke test)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from pipeline.integrated_pipeline import (
    count_real_companies,
    get_real_company_urls,
    mark_visited,
    run_processor,
    save_pagedata,
    scrape_batch,
)

print("🧪 Testing pipeline with 10 companies...\n")

# Check total
total = count_real_companies(visited=False)
print(f"Total companies available: {total}")

if total < 10:
    print("Not enough companies!")
    sys.exit(1)

# Get 10 URLs
url_data = get_real_company_urls(limit=10)
ids = [x[0] for x in url_data]
urls = [x[1] for x in url_data]
id_map = {url: sid for sid, url in url_data}

print(f"\nSample URLs to scrape:")
for url in urls[:5]:
    print(f"  - {url}")
print(f"  ... and {len(urls) - 5} more\n")

# Scrape
print("📥 Scraping...")
pages = scrape_batch(urls)
print(f"✓ Got {len(pages)} results\n")

# Save
print("💾 Saving...")
saved = save_pagedata(pages, id_map)
mark_visited(ids)
print(f"✓ Saved {saved} pages\n")

# Process
print("⚙️  Processing...")
success = run_processor()

if success:
    print("\n✅ Test pipeline PASSED!")
else:
    print("\n❌ Test pipeline FAILED!")
    sys.exit(1)
