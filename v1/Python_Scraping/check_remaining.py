"""Check remaining URLs to scrape."""

import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = (
    Path(__file__).resolve().parent.parent / "Sqlite_Database" / "data" / "yc.sqlite"
)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def check(pattern: Optional[str] = None, exclude: Optional[str] = None):
    """Show remaining URLs for a pattern."""
    conn = get_conn()
    cursor = conn.cursor()

    conditions = []
    params = []

    if pattern:
        conditions.append("url LIKE ?")
        params.append(f"%{pattern}%")
    if exclude:
        conditions.append("url NOT LIKE ?")
        params.append(f"%{exclude}%")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    cursor.execute(
        f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN visited = 1 THEN 1 ELSE 0 END) as visited,
               SUM(CASE WHEN visited = 0 THEN 1 ELSE 0 END) as remaining
        FROM websites_from_sitemap {where}
    """,
        params,
    )

    row = cursor.fetchone()
    conn.close()

    total = row["total"]
    visited = row["visited"] or 0
    remaining = row["remaining"] or 0
    pct = (visited / total * 100) if total > 0 else 0

    label = f"'{pattern}'" if pattern else "all URLs"
    if exclude:
        label += f" (excluding '{exclude}')"
    print(f"{label}: {remaining} left of {total} ({visited} done, {pct:.1f}%)")
    return {"total": total, "visited": visited, "remaining": remaining}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("pattern", nargs="?", help="URL pattern to match")
    parser.add_argument("--exclude", "-x", help="URL pattern to exclude")
    args = parser.parse_args()
    check(args.pattern, args.exclude)
