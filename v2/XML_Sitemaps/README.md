# XML_Sitemaps

Tools and cached XML for seeding the scrape target list.

- `sitemap_parser.py` – httpx + tenacity loader, supports sitemap indexes, local caches, and subset filters; writes to `websites_from_sitemap` via `SQLiteStore`.
- `cli.py` – Typer commands for `load` (local by default, `--remote` for live) and `stats`.

Usage:
```bash
python -m XML_Sitemaps.cli load            # cached XMLs
python -m XML_Sitemaps.cli load --remote   # fetch live
python -m XML_Sitemaps.cli load --only companies jobs
python -m XML_Sitemaps.cli stats
```

Output table:
- `websites_from_sitemap` with columns `(url, lastmod, sitemap_source, visited, visited_at, created_at)`.
