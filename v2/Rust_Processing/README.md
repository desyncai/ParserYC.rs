# Rust_Processing

Rust binaries that turn scraped YC pages into normalized SQLite tables.

What it reads/writes
- Reads from `pagedataobjects` (companies) and `jobs_page_data` (jobs).
- Writes: `companies`, `founders`, `tags`, `news`, `links`, plus job tables (`job_meta`, `job_sections`, `job_body`, `job_text_shortened`, `job_stats`).
- Metrics tables: `company_pass_metrics` + `company_text_residual`.

Company pipeline (`company_metadata_extraction/`)
- Stages: slug+name -> batch/status/location -> tagline/sidebar -> tags -> founders -> news -> links.
- Tracks char counts per pass and residual samples.
- Uses tracing + config; rayon parallel mapping enabled by default.

Jobs pipeline (`jobs_extraction/`)
- Pass 1: shorten text (length stats printed).
- Pass 2: metadata.
- Pass 3: sections/body.
- Pass 4: stats summary.

Dependencies
- `rusqlite`, `serde`, `serde_json`, `regex`, `anyhow`.
- Added: `reqwest` (blocking), `config`, `tracing`/`tracing-subscriber`, `thiserror`, `itertools`, `rayon` (default-enabled feature).

Run it
```bash
cd Rust_Processing
# Companies
YC_DB_PATH=../Sqlite_Database/data/yc.sqlite cargo run --release --bin company_metadata_extraction
# Jobs
YC_DB_PATH=../Sqlite_Database/data/yc.sqlite cargo run --release --bin jobs_extraction
# Disable rayon if needed
cargo run --no-default-features --bin company_metadata_extraction
```

Notes
- Default DB path inside `db.rs` resolves relative to `Sqlite_Database/data/yc.sqlite`; override with `YC_DB_PATH`.
- Ensure Python has scraped pages into `pagedataobjects` / `jobs_page_data` before running.
