from __future__ import annotations

import typer

from Python_Scraping.jobs.loader import init_jobs_page_data
from Python_Scraping.jobs.pipeline import (
    compare_with_jobs_sitemap,
    process_jobs_batch,
    run_jobs_pipeline,
)

app = typer.Typer(help="Jobs scraping pipeline")


@app.command()
def init(
    drop_existing: bool = typer.Option(True, help="Drop and recreate jobs_page_data"),
):
    init_jobs_page_data(drop_existing=drop_existing)


@app.command()
def batch(
    batch_size: int = typer.Option(300, help="Batch size"),
    wait_time: int | None = typer.Option(None, help="Override Desync wait_time"),
):
    process_jobs_batch(batch_size=batch_size, wait_time=wait_time)


@app.command()
def pipeline(
    batch_size: int = typer.Option(300, help="Batch size"),
    wait_time: int | None = typer.Option(None, help="Override Desync wait_time"),
):
    run_jobs_pipeline(batch_size=batch_size, wait_time=wait_time)


@app.command("compare-sitemap")
def compare_sitemap():
    compare_with_jobs_sitemap()


if __name__ == "__main__":
    app()
