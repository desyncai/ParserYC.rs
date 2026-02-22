from __future__ import annotations

import typer

from Python_Scraping.pipeline.coordinator import ScrapeCoordinator

app = typer.Typer(help="Company scraping pipeline")


def _coordinator(batch_size: int, wait_time: int | None) -> ScrapeCoordinator:
    return ScrapeCoordinator(batch_size=batch_size, wait_time=wait_time)


@app.command()
def batch(
    pattern: str | None = typer.Option(None, help="URL substring filter"),
    exclude: str | None = typer.Option(None, help="URL substring exclusion"),
    batch_size: int = typer.Option(100, help="Batch size"),
    wait_time: int | None = typer.Option(None, help="Override Desync wait_time"),
    real: bool = typer.Option(False, help="Use strict real-company filters"),
):
    coord = _coordinator(batch_size, wait_time)
    coord.run_batch(pattern=pattern, exclude=exclude, use_real_company_filter=real)


@app.command()
def pipeline(
    pattern: str | None = typer.Option(None, help="URL substring filter"),
    exclude: str | None = typer.Option(None, help="URL substring exclusion"),
    batch_size: int = typer.Option(100, help="Batch size"),
    max_batches: int | None = typer.Option(None, help="Stop after N batches"),
    wait_time: int | None = typer.Option(None, help="Override Desync wait_time"),
    real: bool = typer.Option(False, help="Use strict real-company filters"),
):
    coord = _coordinator(batch_size, wait_time)
    coord.run_pipeline(
        pattern=pattern,
        exclude=exclude,
        max_batches=max_batches,
        use_real_company_filter=real,
    )


@app.command()
def stats():
    coord = _coordinator(batch_size=1, wait_time=None)
    coord.stats()


if __name__ == "__main__":
    app()
