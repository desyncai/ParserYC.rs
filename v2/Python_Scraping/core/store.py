from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from Python_Scraping.core.filters import Clause
from Python_Scraping.core.logging import warn
from Python_Scraping.core.models import (
    JobPageModel,
    PageDataModel,
    SitemapEntryModel,
    UrlRecordModel,
)

try:
    from Sqlite_Database import schema as db_schema
except Exception:  # fallback if import path differs when run as module
    db_schema = None


class SQLiteStore:
    """Encapsulates SQLite operations for sitemap, pagedata, and jobs tables."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        if db_path is not None:
            self.db_path = Path(db_path)
        elif db_schema and getattr(db_schema, "DB_PATH", None):
            self.db_path = Path(db_schema.DB_PATH)
        else:
            # fallback relative path
            self.db_path = (
                Path(__file__).resolve().parents[2]
                / "Sqlite_Database"
                / "data"
                / "yc.sqlite"
            )
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # --- sitemap helpers ---
    def fetch_urls(
        self,
        *,
        pattern: Optional[str] = None,
        exclude: Optional[str] = None,
        limit: int = 100,
        only_unvisited: bool = True,
        table: str = "websites_from_sitemap",
        extra_clauses: Sequence[Clause] | None = None,
    ) -> List[UrlRecordModel]:
        conditions = []
        params: List[str] = []
        if only_unvisited:
            conditions.append("visited = 0")
        if pattern:
            conditions.append("url LIKE ?")
            params.append(f"%{pattern}%")
        if exclude:
            conditions.append("url NOT LIKE ?")
            params.append(f"%{exclude}%")
        if extra_clauses:
            for col, val in extra_clauses:
                conditions.append(col)
                params.append(val)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT id, url, lastmod, sitemap_source, visited, visited_at
                FROM {table}
                {where}
                ORDER BY id
                LIMIT ?
                """,
                [*params, limit],
            )
            rows = cur.fetchall()

        return [
            UrlRecordModel(
                id=row["id"],
                url=row["url"],
                lastmod=row["lastmod"],
                sitemap_source=row["sitemap_source"],
                visited=row["visited"],
                visited_at=row["visited_at"],
            )
            for row in rows
        ]

    def count_urls(
        self,
        *,
        pattern: Optional[str] = None,
        exclude: Optional[str] = None,
        visited: Optional[bool] = None,
        table: str = "websites_from_sitemap",
        extra_clauses: Sequence[Clause] | None = None,
    ) -> int:
        conditions = []
        params: List[str] = []
        if pattern:
            conditions.append("url LIKE ?")
            params.append(f"%{pattern}%")
        if exclude:
            conditions.append("url NOT LIKE ?")
            params.append(f"%{exclude}%")
        if visited is not None:
            conditions.append("visited = ?")
            params.append(1 if visited else 0)
        if extra_clauses:
            for col, val in extra_clauses:
                conditions.append(col)
                params.append(val)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table} {where}", params)
            return cur.fetchone()[0]

    def mark_visited(
        self, ids: Iterable[int], *, table: str = "websites_from_sitemap"
    ) -> None:
        id_list = list(ids)
        if not id_list:
            return
        with self.connect() as conn:
            cur = conn.cursor()
            cur.executemany(
                f"""
                UPDATE {table}
                SET visited = 1, visited_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                [(i,) for i in id_list],
            )
            conn.commit()

    def insert_sitemap_entries(self, entries: Sequence[SitemapEntryModel]) -> int:
        if not entries:
            return 0
        inserted = 0
        with self.connect() as conn:
            cur = conn.cursor()
            for entry in entries:
                try:
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO websites_from_sitemap (url, lastmod, sitemap_source)
                        VALUES (?, ?, ?)
                        """,
                        (entry.url, entry.lastmod, entry.source),
                    )
                    if cur.rowcount:
                        inserted += 1
                except Exception as exc:
                    warn(f"  Error inserting {entry.url}: {exc}")
            conn.commit()
        return inserted

    # --- pagedata helpers ---
    def save_pagedata(self, pages: Sequence, id_map: dict[str, int]) -> int:
        if not pages:
            return 0
        with self.connect() as conn:
            cur = conn.cursor()
            saved = 0
            for p in pages:
                model = PageDataModel.from_obj(p)
                try:
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO pagedataobjects (
                            id, url, domain, timestamp, bulk_search_id, search_type,
                            text_content, html_content, internal_links, external_links,
                            latency_ms, complete, created_at, sitemap_id, scraped_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            model.id,
                            model.url,
                            model.domain,
                            model.timestamp,
                            model.bulk_search_id,
                            model.search_type,
                            model.text_content,
                            model.html_content,
                            json.dumps(model.internal_links)
                            if model.internal_links
                            else None,
                            json.dumps(model.external_links)
                            if model.external_links
                            else None,
                            model.latency_ms,
                            1 if model.complete else 0,
                            model.created_at,
                            id_map.get(model.url),
                        ),
                    )
                    saved += 1
                except Exception as exc:
                    warn(f"  Error saving {model.url}: {exc}")
            conn.commit()
            return saved

    # --- jobs helpers ---
    def fetch_job_batch(self, *, limit: int = 300) -> list[tuple[int, str]]:
        with self.connect() as conn:
            cur = conn.cursor()
            rows = cur.execute(
                """
                SELECT job_id, url
                FROM jobs_page_data
                WHERE visited = 0
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [(row["job_id"], row["url"]) for row in rows]

    def mark_jobs_visited(self, job_ids: Iterable[int]) -> None:
        ids = list(job_ids)
        if not ids:
            return
        with self.connect() as conn:
            cur = conn.cursor()
            cur.executemany(
                """
                UPDATE jobs_page_data
                SET visited = 1, visited_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
                """,
                [(i,) for i in ids],
            )
            conn.commit()

    def save_job_pages(self, pages: Sequence, id_map: dict[str, int]) -> int:
        if not pages:
            return 0
        with self.connect() as conn:
            cur = conn.cursor()
            saved = 0
            for p in pages:
                model = JobPageModel.from_page(p, id_map.get(getattr(p, "url", None)))
                if model.job_id is None:
                    continue
                cur.execute(
                    """
                    UPDATE jobs_page_data
                    SET desync_id = ?, domain = ?, timestamp = ?, bulk_search_id = ?,
                        search_type = ?, text_content = ?, html_content = ?,
                        internal_links = ?, external_links = ?, latency_ms = ?,
                        complete = ?, created_at = ?, scraped_at = CURRENT_TIMESTAMP
                    WHERE job_id = ?
                    """,
                    (
                        model.desync_id,
                        model.domain,
                        model.timestamp,
                        model.bulk_search_id,
                        model.search_type,
                        model.text_content,
                        model.html_content,
                        json.dumps(model.internal_links)
                        if model.internal_links
                        else None,
                        json.dumps(model.external_links)
                        if model.external_links
                        else None,
                        model.latency_ms,
                        1 if model.complete else 0,
                        model.created_at,
                        model.job_id,
                    ),
                )
                saved += cur.rowcount
            conn.commit()
            return saved

    def count_jobs(self, *, visited: bool | None = None) -> int:
        with self.connect() as conn:
            cur = conn.cursor()
            if visited is None:
                row = cur.execute("SELECT COUNT(*) FROM jobs_page_data").fetchone()
            else:
                row = cur.execute(
                    "SELECT COUNT(*) FROM jobs_page_data WHERE visited = ?",
                    (1 if visited else 0,),
                ).fetchone()
        return row[0] if row else 0

    def seed_job_urls(
        self, url_pairs: Sequence[tuple[str, int]], *, drop_existing: bool = True
    ) -> int:
        if not url_pairs:
            return 0
        with self.connect() as conn:
            cur = conn.cursor()
            if drop_existing:
                cur.execute("DROP TABLE IF EXISTS jobs_page_data")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs_page_data (
                    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    visited INTEGER DEFAULT 0,
                    visited_at TEXT,
                    sitemap_id INTEGER,
                    desync_id INTEGER,
                    domain TEXT,
                    timestamp INTEGER,
                    bulk_search_id TEXT,
                    search_type TEXT,
                    text_content TEXT,
                    html_content TEXT,
                    internal_links TEXT,
                    external_links TEXT,
                    latency_ms INTEGER,
                    complete INTEGER,
                    created_at INTEGER,
                    scraped_at TEXT,
                    FOREIGN KEY (sitemap_id) REFERENCES websites_from_sitemap(id)
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_url ON jobs_page_data(url)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_visited ON jobs_page_data(visited)"
            )
            before = cur.execute("SELECT COUNT(*) FROM jobs_page_data").fetchone()[0]
            cur.executemany(
                """
                INSERT OR IGNORE INTO jobs_page_data (url, sitemap_id, visited)
                VALUES (?, ?, 0)
                """,
                [(url, sid) for url, sid in url_pairs],
            )
            after = cur.execute("SELECT COUNT(*) FROM jobs_page_data").fetchone()[0]
            conn.commit()
            return after - before


default_store = SQLiteStore()
