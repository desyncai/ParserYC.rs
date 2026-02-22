# pipeline

Python orchestrator tying scraping to the Rust processor.

- `integrated_pipeline.py`:
  - Pulls batches of real company URLs from `websites_from_sitemap` (filters out category/tag/job/launch pages).
  - Calls `Python_Scraping.yc_scraper.scrape_batch` to fetch content.
  - Saves into `pagedataobjects` and marks the source URLs as visited.
  - Runs the Rust processor (`Rust_Processing/target/release/processing_tech`) with `YC_DB_PATH` pointing to `Sqlite_Database/data/yc.sqlite`.

Usage:
```bash
export DESYNC_API_KEY=...   # required for scraping
python pipeline/integrated_pipeline.py
```

Config knobs:
- `BATCH_SIZE` in the script (default 300).
- Rust binary path is derived from `Rust_Processing`; override DB path via `YC_DB_PATH` if needed.
