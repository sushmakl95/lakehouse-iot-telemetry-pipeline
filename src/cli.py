"""Unified CLI: `iot-pipeline [bronze|silver|gold|optimize] --config <path>`."""

from __future__ import annotations

import sys

import click

from src.bronze import main as bronze_main
from src.gold import main as gold_main
from src.silver import main as silver_main


@click.group()
@click.version_option(version="1.0.0")
def cli() -> None:
    """Lakehouse IoT Telemetry Pipeline CLI."""


@cli.command()
@click.option("--config", required=True, type=click.Path(exists=True))
def bronze(config: str) -> None:
    """Run Bronze layer ingestion."""
    sys.exit(bronze_main(config))


@cli.command()
@click.option("--config", required=True, type=click.Path(exists=True))
def silver(config: str) -> None:
    """Run Silver layer transformation."""
    sys.exit(silver_main(config))


@cli.command()
@click.option("--config", required=True, type=click.Path(exists=True))
def gold(config: str) -> None:
    """Run Gold layer aggregation."""
    sys.exit(gold_main(config))


@cli.command()
@click.option("--config", required=True, type=click.Path(exists=True))
def run_all(config: str) -> None:
    """Run Bronze → Silver → Gold in sequence."""
    for stage, fn in [("bronze", bronze_main), ("silver", silver_main), ("gold", gold_main)]:
        click.echo(f"--- Running {stage} ---")
        rc = fn(config)
        if rc != 0:
            click.echo(f"[x] {stage} failed, stopping", err=True)
            sys.exit(rc)
    click.echo("[ok] All stages completed")


if __name__ == "__main__":
    cli()
