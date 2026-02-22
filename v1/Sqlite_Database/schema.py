"""
Database schema for YC website scraping project.
Creates and manages the SQLite database at Sqlite_Database/data/yc.sqlite
"""

import sqlite3
from pathlib import Path

# Relative path from this file's location
DB_PATH = Path(__file__).resolve().parent / "data" / "yc.sqlite"


def get_connection() -> sqlite3.Connection:
    """Get a connection to the SQLite database."""
    # Ensure the data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def create_tables():
    """Create the database tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    # Table for URLs from sitemap
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS websites_from_sitemap (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            lastmod TEXT,
            sitemap_source TEXT,
            visited INTEGER DEFAULT 0,
            visited_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Table for PageData objects from Desync AI
    cursor.execute("""
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
    """)

    # Create indexes for faster lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_sitemap_visited 
        ON websites_from_sitemap(visited)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_sitemap_source 
        ON websites_from_sitemap(sitemap_source)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pagedata_domain 
        ON pagedataobjects(domain)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pagedata_complete 
        ON pagedataobjects(complete)
    """)

    conn.commit()
    conn.close()
    print(f"Database created/updated at: {DB_PATH}")


def reset_tables():
    """Drop and recreate all tables. Use with caution."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS pagedataobjects")
    cursor.execute("DROP TABLE IF EXISTS websites_from_sitemap")
    conn.commit()
    conn.close()
    create_tables()
    print("Tables reset successfully.")


if __name__ == "__main__":
    create_tables()
