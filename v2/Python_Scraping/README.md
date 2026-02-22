# Python_Scraping

Modular scraping helpers and CLIs built on shared core utilities.

## Core modules
- `core/desync_client.py` – `DesyncScraper` wrapper (httpx + tenacity retries, chunking, wait-time/html toggles).
- `core/store.py` – `SQLiteStore` for sitemap/pagedata/jobs, shared DB path, JSON handling.
- `core/models.py` – pydantic DTOs for PageData, sitemap entries, job pages.
- `core/filters.py` – reusable URL filters (real companies, jobs).
- `core/logging.py` – Rich console formatting.

## Pipelines
- `pipeline/coordinator.py` – company scraper orchestration; Typer CLI in `pipeline/cli.py`.
- `jobs/loader.py` & `jobs/pipeline.py` – job seeding and scraping; Typer CLI in `jobs/cli.py`.
- `XML_Sitemaps/` – sitemap loader (see `XML_Sitemaps/cli.py`).

## Usage (common)
- Set `DESYNC_API_KEY`.
- Create tables: `python -m Sqlite_Database.schema` (or `--reset`).

### Companies
```bash
python -m XML_Sitemaps.cli load        # seed sitemap URLs (local caches by default)
python -m Python_Scraping.pipeline.cli pipeline --real --batch-size 300
```

### Jobs
```bash
python -m Python_Scraping.jobs.cli init
python -m Python_Scraping.jobs.cli pipeline --batch-size 300 --wait-time 10
python -m Python_Scraping.jobs.cli compare-sitemap
```

### Integrated
- `python pipeline/integrated_pipeline.py` scrapes real companies in batches, then runs Rust processor each batch.

### Testing/Smoke
- `python Python_Scraping/test_pipeline.py` (requires API key + DB with URLs).
