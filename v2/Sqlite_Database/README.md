# Sqlite_Database

Holds the working SQLite file and schema helpers.

- Database file: `data/yc.sqlite` (exposed via `db_path()` in `schema.py`).
- Schema helper: `schema.py` exposes `create_tables()`, `reset_tables()`, and `DB_PATH`/`db_path()`.

Tables (core + metrics + jobs)
- `websites_from_sitemap` – source URLs with metadata.
- `pagedataobjects` – scraped YC pages.
- `jobs_page_data` – seeded job URLs + scrape responses.
- `job_text_shortened`, `job_meta`, `job_body`, `job_sections`, `job_stats` – produced by the Rust jobs pipeline.
- `company_pass_metrics`, `company_text_residual` – per-pass character metrics and samples for the company pipeline.

Helpers that write/read
- Python: `SQLiteStore` (core/store.py) handles saves and visited flags; job pipeline updates `jobs_page_data`.
- Rust: `company_metadata_extraction` writes company metadata + metrics; `jobs_extraction` writes job tables.

Initialize:
```bash
python -m Sqlite_Database.schema      # creates/updates all tables
python -m Sqlite_Database.schema --reset  # drop + recreate (use cautiously)
```
