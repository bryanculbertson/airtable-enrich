#!/usr/bin/env python

"""
Entry point for running all data enrichment commands.
"""
import os

import airtable
import click
import dotenv


@click.group()
def cli():
    """Run airtable-enrich"""
    pass


@cli.command()
@click.option("--base", "basekey", type=str, prompt=True)
@click.option("--table", "tablename", type=str, prompt=True)
@click.option(
    "--apikey", type=str, default=lambda: os.environ.get("AIRTABLE_APIKEY", "")
)
@click.option("--limit", type=int, default=5)
def head(basekey: str, tablename: str, apikey: str, limit: int):
    """Print the head of the table"""
    table = airtable.Airtable(basekey, tablename, apikey)
    for row in table.get_all(maxRecords=limit):
        click.echo(row)


@cli.command()
@click.option("--base", "basekey", type=str, prompt=True)
@click.option("--table", "tablename", type=str, prompt=True)
@click.option(
    "--apikey", type=str, default=lambda: os.environ.get("AIRTABLE_APIKEY", "")
)
def fields(basekey: str, tablename: str, apikey: str):
    """Print the head of the table"""
    table = airtable.Airtable(basekey, tablename, apikey)
    for row in table.get_all(maxRecords=1):
        click.echo(list(row['fields'].keys()))


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    dotenv.load_dotenv()
    cli()
