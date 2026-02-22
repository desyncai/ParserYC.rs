# Desync Data Processing Extension

Modular, reusable pipeline for YC scraping and processing with shared core utilities.

## Layout
- `Python_Scraping/core/` – shared `DesyncScraper` (httpx + tenacity), `SQLiteStore`, pydantic models, filters, rich logging.
- `Python_Scraping/pipeline/` – company `ScrapeCoordinator` + Typer CLI (`python -m Python_Scraping.pipeline.cli`).
- `Python_Scraping/jobs/` – job loader/pipeline + Typer CLI (`python -m Python_Scraping.jobs.cli`).
- `XML_Sitemaps/` – sitemap parser/loader + Typer CLI (`python -m XML_Sitemaps.cli`).
- `Sqlite_Database/` – schema and DB path helper.
- `pipeline/` – integrated orchestrator that scrapes then calls Rust processor.
- `Rust_Processing/` – Rust binaries (`company_metadata_extraction`, `jobs_extraction`) with tracing/config and optional rayon parallelism.

## Setup
1) Python deps (fast): `uv pip install -r requirements.txt` (or `pip install -r requirements.txt`).
2) Env: set `DESYNC_API_KEY` for Desync requests.
3) DB: `python -m Sqlite_Database.schema` (or `--reset`).
4) Rust toolchain: `cargo build --release` inside `Rust_Processing` (enables rayon by default).

## Commands (Python)
- Sitemaps: `python -m XML_Sitemaps.cli load --remote` (or omit `--remote` to use cached XMLs); `python -m XML_Sitemaps.cli stats`.
- Companies: `python -m Python_Scraping.pipeline.cli pipeline --real --batch-size 300` (or `batch`/`stats`).
- Jobs: `python -m Python_Scraping.jobs.cli init` then `python -m Python_Scraping.jobs.cli pipeline --batch-size 300` (or `batch`, `compare-sitemap`).
- Integrated loop: `python pipeline/integrated_pipeline.py` (scrape real companies in batches, run Rust processor after each batch).

## Commands (Rust)
```bash
cd Rust_Processing
# Companies
YC_DB_PATH=../Sqlite_Database/data/yc.sqlite cargo run --release --bin company_metadata_extraction
# Jobs
YC_DB_PATH=../Sqlite_Database/data/yc.sqlite cargo run --release --bin jobs_extraction
```
- Uses `config` + env (prefix `YC`) and `tracing` for structured logs.
- Rayon enabled by default; disable with `cargo run --no-default-features --bin company_metadata_extraction` if needed.

## Data flow
Sitemaps → `websites_from_sitemap` → Python scrapers (`pagedataobjects`/`jobs_page_data`) → Rust processors → structured tables + metrics.
