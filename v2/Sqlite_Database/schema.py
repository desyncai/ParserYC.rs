"""
Database schema for YC scraping + processing.
Creates and manages the SQLite database at Sqlite_Database/data/yc.sqlite
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

# Relative path from this file's location
DB_PATH = Path(__file__).resolve().parent / "data" / "yc.sqlite"


def db_path() -> Path:
    """Return the configured database path (Path object)."""
    return DB_PATH


TABLE_SQL = {
    "websites_from_sitemap": """
        CREATE TABLE IF NOT EXISTS websites_from_sitemap (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            lastmod TEXT,
            sitemap_source TEXT,
            visited INTEGER DEFAULT 0,
            visited_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "pagedataobjects": """
        CREATE TABLE IF NOT EXISTS pagedataobjects (
            id INTEGER PRIMARY KEY,
            url TEXT UNIQUE NOT NULL,
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
            scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
            sitemap_id INTEGER,
            FOREIGN KEY (sitemap_id) REFERENCES websites_from_sitemap(id)
        )
    """,
    "jobs_page_data": """
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
    """,
    "company_pass_metrics": """
        CREATE TABLE IF NOT EXISTS company_pass_metrics (
            run_id TEXT NOT NULL,
            pass_name TEXT NOT NULL,
            pages INTEGER,
            chars_before INTEGER,
            chars_after INTEGER,
            chars_removed INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, pass_name)
        )
    """,
    "company_text_residual": """
        CREATE TABLE IF NOT EXISTS company_text_residual (
            run_id TEXT NOT NULL,
            company_slug TEXT NOT NULL,
            pass_name TEXT NOT NULL,
            remaining_chars INTEGER,
            sample TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, company_slug, pass_name)
        )
    """,
    "job_text_shortened": """
        CREATE TABLE IF NOT EXISTS job_text_shortened (
            job_id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            company_slug TEXT,
            job_slug TEXT,
            text_shortened TEXT,
            raw_len INTEGER,
            shortened_len INTEGER,
            is_blank INTEGER,
            is_404 INTEGER,
            nav_removed INTEGER,
            similar_removed INTEGER,
            footer_removed INTEGER,
            founder_removed INTEGER,
            scraped_at TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs_page_data(job_id) ON DELETE CASCADE
        )
    """,
    "job_meta": """
        CREATE TABLE IF NOT EXISTS job_meta (
            job_id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            company_slug TEXT,
            job_slug TEXT,
            job_title TEXT,
            role_raw TEXT,
            role_bucket TEXT,
            job_type TEXT,
            position_type TEXT,
            location_raw TEXT,
            pay_raw TEXT,
            experience_raw TEXT,
            visa_raw TEXT,
            has_emoji INTEGER,
            header_ok INTEGER,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs_page_data(job_id) ON DELETE CASCADE
        )
    """,
    "job_body": """
        CREATE TABLE IF NOT EXISTS job_body (
            job_id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            role_description TEXT,
            body_ok INTEGER,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs_page_data(job_id) ON DELETE CASCADE
        )
    """,
    "job_sections": """
        CREATE TABLE IF NOT EXISTS job_sections (
            job_id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            responsibilities TEXT,
            requirements TEXT,
            nice_to_have TEXT,
            benefits TEXT,
            summary TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs_page_data(job_id) ON DELETE CASCADE
        )
    """,
    "job_stats": """
        CREATE TABLE IF NOT EXISTS job_stats (
            metric TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """,
}

INDEX_SQL = [
    ("idx_sitemap_visited", "websites_from_sitemap", "visited"),
    ("idx_sitemap_source", "websites_from_sitemap", "sitemap_source"),
    ("idx_pagedata_domain", "pagedataobjects", "domain"),
    ("idx_pagedata_complete", "pagedataobjects", "complete"),
    ("idx_jobs_url", "jobs_page_data", "url"),
    ("idx_jobs_visited", "jobs_page_data", "visited"),
    ("idx_job_text_company", "job_text_shortened", "company_slug"),
    ("idx_job_meta_bucket", "job_meta", "role_bucket"),
]


def get_connection() -> sqlite3.Connection:
    """Get a connection to the SQLite database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def create_tables() -> None:
    """Create the database tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    for name, sql in TABLE_SQL.items():
        cursor.execute(sql)

    for idx, table, cols in INDEX_SQL:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx} ON {table} ({cols})")

    conn.commit()
    conn.close()
    print(f"Database created/updated at: {DB_PATH}")


def reset_tables() -> None:
    """Drop and recreate all tables. Use with caution."""
    conn = get_connection()
    cursor = conn.cursor()
    for name in TABLE_SQL:
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
    conn.commit()
    conn.close()
    create_tables()
    print("Tables reset successfully.")


def ensure_columns(table: str, columns: Iterable[tuple[str, str]]) -> None:
    """
    Best-effort helper to add missing columns without blowing away user data.
    columns: iterable of (name, definition) pairs; definition should be a valid SQL snippet.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        existing = {row["name"] for row in cur.fetchall()}
        for name, definition in columns:
            if name in existing:
                continue
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
            except sqlite3.Error as exc:
                print(f"Could not add column {name} to {table}: {exc}")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SQLite schema bootstrapper")
    parser.add_argument(
        "--reset", action="store_true", help="Drop and recreate all tables"
    )
    args = parser.parse_args()

    if args.reset:
        reset_tables()
    else:
        create_tables()
