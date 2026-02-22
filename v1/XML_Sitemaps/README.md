# XML_Sitemaps

Tools and cached XML for seeding the scrape target list.

- `sitemap_parser.py` – fetches YC sitemaps (main, companies, library, launches, jobs), parses URLs/lastmod, and inserts them into `websites_from_sitemap` via `Sqlite_Database.schema`.
- Cached XMLs: `companies_sitemap.xml`, `jobs_sitemap.xml`, `launches_sitemap.xml` for offline loads.

Usage:
- Live fetch: `python XML_Sitemaps/sitemap_parser.py` (requires network).
- Offline: `python XML_Sitemaps/sitemap_parser.py --local` (uses the cached XML files in this directory).

Output tables:
- `websites_from_sitemap` with columns `(url, lastmod, sitemap_source, visited, visited_at, created_at)`.
