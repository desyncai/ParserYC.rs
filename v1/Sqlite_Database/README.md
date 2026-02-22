# Sqlite_Database

Holds the working SQLite file and basic schema helpers.

- Database file: `data/yc.sqlite`.
- Schema helper: `schema.py` exposes `get_connection()`, `create_tables()`, `reset_tables()`, and `DB_PATH`.

Tables (core):
- `websites_from_sitemap` – source URLs with metadata: `(id, url, lastmod, sitemap_source, visited, visited_at, created_at)`.
- `pagedataobjects` – scraped YC pages (companies/launches/library): `(id, url, domain, timestamp, bulk_search_id, search_type, text_content, html_content, internal_links, external_links, latency_ms, complete, created_at, scraped_at, sitemap_id)`.
- `jobs_page_data` – created by `Python_Scraping/job_listings/scrape_jobs.py`; tracks job URLs, Desync metadata, and a `visited` flag so batches aren’t re-enqueued.

Helpers that write/read:
- Python: `Python_Scraping/yc_scraper.py` handles saves to `pagedataobjects` and marking `websites_from_sitemap.visited`. `Python_Scraping/job_listings/scrape_jobs.py` seeds/updates `jobs_page_data`.
- Rust: `Rust_Processing/src/db.rs` provides `connect`, `insert`, `insert_batch`, `update`, and `fetch_pages`, and auto-creates tables defined in `schema.json` when run.

Initialize:
```bash
python -m Sqlite_Database.schema  # creates core tables if missing
```
