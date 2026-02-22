# pipeline

Python orchestrator tying scraping to the Rust processor.

- `integrated_pipeline.py`: scrapes real company URLs via `ScrapeCoordinator` then runs `Rust_Processing/target/release/company_metadata_extraction` with `YC_DB_PATH` set from `Sqlite_Database/schema.py`.
- `Python_Scraping/pipeline/cli.py`: Typer CLI for batch/pipeline/stats on companies.

Usage:
```bash
export DESYNC_API_KEY=...   # required for scraping
python -m XML_Sitemaps.cli load           # seed sitemap URLs (cached by default)
python -m Python_Scraping.pipeline.cli pipeline --real --batch-size 300
# or run integrated loop
python pipeline/integrated_pipeline.py
```

Config knobs:
- `BATCH_SIZE` in `integrated_pipeline.py` (default 300).
- Override DB path via `YC_DB_PATH` if needed (defaults to `Sqlite_Database/data/yc.sqlite`).
