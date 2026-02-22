"""Check remaining URLs to scrape using the new core store."""

from typing import Optional

from Python_Scraping.core.store import SQLiteStore, default_store


def check(
    pattern: Optional[str] = None,
    exclude: Optional[str] = None,
    store: SQLiteStore = default_store,
):
    total = store.count_urls(pattern=pattern, exclude=exclude, visited=None)
    visited = store.count_urls(pattern=pattern, exclude=exclude, visited=True)
    remaining = total - visited
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
