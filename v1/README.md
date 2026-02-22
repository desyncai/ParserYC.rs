# Desync Data Processing Extension

Compact map of the repo:

- `Python_Scraping/` – Python scrapers and job-runner utilities (YC site + job listings).
- `XML_Sitemaps/` – sitemap fetch/parse loader to seed URLs into SQLite.
- `Sqlite_Database/` – SQLite schema helpers and the working database (`data/yc.sqlite`).
- `pipeline/` – Python orchestrator that ties scraping to Rust post-processing.
- `Rust_Processing/` – Rust binary that parses scraped pages into structured tables.

Quick start:
- Set `DESYNC_API_KEY` in your environment for scraping.
- Ensure the DB exists (run `python -m Sqlite_Database.schema` to create base tables).
- Load URLs: `python XML_Sitemaps/sitemap_parser.py` (or `--local` for cached XMLs).
- Scrape companies: `python pipeline/integrated_pipeline.py` (reads from SQLite, writes back, then calls Rust).
- Scrape job pages: `python Python_Scraping/job_listings/scrape_jobs.py --init --pipeline`.

Data flow:
Sitemaps -> `websites_from_sitemap` -> Python scrapers -> `pagedataobjects` / `jobs_page_data` -> Rust processor -> processed tables.

## SQLite database structure (`Sqlite_Database/data/yc.sqlite`)
- `websites_from_sitemap` - sitemap URLs. Columns: `id` (PK, autoincrement), `url` (unique, required), `lastmod`, `sitemap_source`, `visited` (default 0), `visited_at`, `created_at` (default CURRENT_TIMESTAMP). Indexes: `idx_sitemap_visited` (`visited`), `idx_sitemap_source` (`sitemap_source`).
- `pagedataobjects` - scraped company pages. Columns: `id` (PK), `url` (unique, required), `domain`, `timestamp`, `bulk_search_id`, `search_type`, `text_content`, `html_content`, `internal_links`, `external_links`, `latency_ms`, `complete`, `created_at`, `scraped_at` (default CURRENT_TIMESTAMP), `sitemap_id` (FK -> `websites_from_sitemap.id`). Indexes: `idx_pagedata_domain` (`domain`), `idx_pagedata_complete` (`complete`).
- `jobs_page_data` - scraped job listings. Columns: `job_id` (PK, autoincrement), `url` (unique, required), `visited` (default 0), `visited_at`, `sitemap_id` (FK -> `websites_from_sitemap.id`), `desync_id`, `domain`, `timestamp`, `bulk_search_id`, `search_type`, `text_content`, `html_content`, `internal_links`, `external_links`, `latency_ms`, `complete`, `created_at`, `scraped_at`. Indexes: `idx_jobs_url` (`url`), `idx_jobs_visited` (`visited`).
- `companies` - YC company metadata. Columns: `slug` (PK), `name` (required), `tagline`, `batch_season`, `batch_year`, `status`, `location`, `founded_year`, `team_size`, `primary_partner`, `job_count` (default 0), `is_hiring` (default 0), `source_url` (required). Indexes: `idx_companies_batch` (`batch_season`, `batch_year`), `idx_companies_status` (`status`), `idx_companies_location` (`location`).
- `founders` - founders per company. Columns: `id` (PK, autoincrement), `company_slug` (FK -> `companies.slug`), `name` (required), `title`; constraint: unique (`company_slug`, `name`). Indexes: `idx_founders_company` (`company_slug`).
- `links` - company and founder links. Columns: `id` (PK, autoincrement), `company_slug` (FK -> `companies.slug`), `founder_id` (FK -> `founders.id`, ON DELETE SET NULL), `url` (required), `pattern`; constraint: unique (`company_slug`, `url`). Indexes: `idx_links_company` (`company_slug`), `idx_links_pattern` (`pattern`), `idx_links_founder` (`founder_id`).
- `news` - company news items. Columns: `id` (PK, autoincrement), `company_slug` (FK -> `companies.slug`), `title` (required), `source`, `published_date`; constraint: unique (`company_slug`, `title`). Indexes: `idx_news_company` (`company_slug`).
- `tags` - YC tags. Columns: `company_slug` (FK -> `companies.slug`), `tag`; primary key: (`company_slug`, `tag`). Indexes: `idx_tags_tag` (`tag`).
- Internal tables: `sqlite_sequence` (tracks AUTOINCREMENT), `sqlite_stat1`, `sqlite_stat4` (ANALYZE stats).
