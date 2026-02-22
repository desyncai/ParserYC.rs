from __future__ import annotations

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

console = Console()


def info(msg: str) -> None:
    console.print(f"[bold cyan]INFO[/]: {msg}")


def warn(msg: str) -> None:
    console.print(f"[bold yellow]WARN[/]: {msg}")


def error(msg: str) -> None:
    console.print(f"[bold red]ERROR[/]: {msg}")


def success(msg: str) -> None:
    console.print(f"[bold green]OK[/]: {msg}")


def progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    )
