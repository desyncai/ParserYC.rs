# Python_Scraping

Scripts that orchestrate scraping and light bookkeeping around the SQLite DB.

- `yc_scraper.py` – core helpers: `get_conn`, `scrape_batch`, `save_pagedata`, `mark_visited`, `run_pipeline`.
- `job_listings/scrape_jobs.py` – builds `jobs_page_data`, tracks visited job URLs, scrapes via Desync in batches, and optionally compares to `XML_Sitemaps/jobs_sitemap.xml`.
- `check_remaining.py` – quick counts of visited/unvisited URLs in `websites_from_sitemap`.
- `test_pipeline.py` – minimal smoke script that exercises the integrated pipeline (expects API key and DB present).

Environment:
- `DESYNC_API_KEY` must be set for any call that hits Desync (`scrape_batch`).
- DB path: `Sqlite_Database/data/yc.sqlite` (wired inside `yc_scraper.py`).

Typical usage:
- Seed sitemaps first (see `XML_Sitemaps/README.md`), then run company scrape via `pipeline/integrated_pipeline.py`.
- Jobs scrape: `python Python_Scraping/job_listings/scrape_jobs.py --init --batch-size 300 --pipeline`.

Testing:
- For a safe check without hitting the API, import modules (e.g., `python - <<'PY'\nfrom Python_Scraping import yc_scraper\nprint(yc_scraper.DB_PATH.exists())\nPY`). Avoid running `scrape_batch` unless `DESYNC_API_KEY` is set.
