# Rust_Processing

Rust binary (`processing_tech`) that turns scraped YC pages into normalized SQLite tables.

What it reads/writes
- Reads from `pagedataobjects` (populated by Python scrapers).
- Writes back to: `companies`, `founders`, `tags`, `news`, `links`.

How it works (8 passes in `src/main.rs`)
- [1] slug + name + source_url
- [2] batch (season + year)
- [3] status + location
- [4] tags (ALL_CAPS with location filter)
- [5] details (tagline, sidebar fields, is_hiring)
- [6] founders (name/title/email/linkedin)
- [7] news items
- [8] external links (classification + founder matching)

Key files
- `src/main.rs` – orchestrates all passes.
- `src/db.rs` – SQLite connector, generic insert/update/batch helpers, and table bootstrap from `schema.json`.
- `schema.json` – table schemas and field mappings used by the processor.

Dependencies (Cargo.toml)
- `rusqlite` (bundled)
- `serde`, `serde_json`
- `regex`
- `anyhow`

Run it
```bash
cd Rust_Processing
YC_DB_PATH=../Sqlite_Database/data/yc.sqlite cargo run         # debug
YC_DB_PATH=../Sqlite_Database/data/yc.sqlite cargo build --release
YC_DB_PATH=../Sqlite_Database/data/yc.sqlite ./target/release/processing_tech
```

Notes
- Default DB path inside `src/db.rs` is `../Sqlite_Database/data/yc.sqlite`; override with `YC_DB_PATH`.
- Ensure Python has scraped pages into `pagedataobjects` before running this processor.
- The processor ignores job-specific tables (`jobs_page_data`); it only needs `pagedataobjects` + `websites_from_sitemap`.
