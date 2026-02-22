from __future__ import annotations

import typer

from XML_Sitemaps.sitemap_parser import SitemapLoader, get_stats

app = typer.Typer(help="Sitemap loader")


@app.command()
def load(
    remote: bool = typer.Option(
        False, help="Fetch live sitemaps instead of cached files"
    ),
    only: list[str] = typer.Option(
        None, help="Subset of sitemaps: main, companies, library, launches, jobs"
    ),
):
    loader = SitemapLoader()
    loader.load_all(use_local=not remote, only=only)


@app.command()
def stats():
    for key, value in get_stats().items():
        typer.echo(f"{key}: {value}")


if __name__ == "__main__":
    app()
