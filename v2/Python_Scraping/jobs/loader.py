from __future__ import annotations

from typing import Iterable

from Python_Scraping.core.filters import JOB_URL_FILTER
from Python_Scraping.core.logging import info
from Python_Scraping.core.store import SQLiteStore, default_store


def _job_url_rows(store: SQLiteStore) -> list[tuple[int, str]]:
    with store.connect() as conn:
        cur = conn.cursor()
        where = " AND ".join([c[0] for c in JOB_URL_FILTER])
        params = [c[1] for c in JOB_URL_FILTER]
        rows = cur.execute(
            f"SELECT id, url FROM websites_from_sitemap WHERE {where}",
            params,
        ).fetchall()
    return [(row["id"], row["url"]) for row in rows]


def init_jobs_page_data(
    *, drop_existing: bool = True, store: SQLiteStore = default_store
) -> int:
    """Create jobs_page_data and seed it from websites_from_sitemap."""
    payload = _job_url_rows(store)
    inserted = store.seed_job_urls(
        [(url, sid) for sid, url in payload], drop_existing=drop_existing
    )
    info(f"Seeded {inserted} job URLs into jobs_page_data")
    return inserted


def jobs_table_exists(store: SQLiteStore = default_store) -> bool:
    with store.connect() as conn:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs_page_data'"
        ).fetchone()
        return bool(row)


def mark_job_urls_visited(
    job_ids: Iterable[int], store: SQLiteStore = default_store
) -> None:
    store.mark_jobs_visited(job_ids)
