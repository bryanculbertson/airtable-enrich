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
    """Print the fields of the table"""
    table = airtable.Airtable(basekey, tablename, apikey)
    for row in table.get_all(maxRecords=1):
        click.echo(list(row['fields'].keys()))


@cli.command()
@click.option("--base", "basekey", type=str, prompt=True)
@click.option("--table", "tablename", type=str, prompt=True)
@click.option(
    "--apikey", type=str, default=lambda: os.environ.get("AIRTABLE_APIKEY", "")
)
@click.option("--lat", "lat_field", type=str, required=True)
@click.option("--lng", "lng_field", type=str, required=True)
@click.option("--tract", "tract_field", type=str, required=True)
@click.option("--force", type=bool, default=False)
def fill_census(
    basekey: str,
    tablename: str,
    apikey: str,
    lat_field: str,
    lng_field: str,
    tract_field: str,
    force: bool,
):
    """Print the head of the table"""
    table = airtable.Airtable(basekey, tablename, apikey)

    # Validate that rows exist and the specified fields are available
    rows = table.get_all(maxRecords=1)
    if not rows:
        click.echo("No rows in table")
        return

    row = rows[0]

    if lat_field not in row["fields"]:
        raise Exception(
            f"Table is missing requred lat field ({lat_field})"
        )

    if lng_field not in row["fields"]:
        raise Exception(
            f"Table is missing requred lng field ({lng_field})"
        )

    if tract_field not in row["fields"]:
        raise Exception(
            f"Table is missing requred tract field ({tract_field})"
        )

    # Download all the rows locally because we are going to modify them in bulk.
    with click.progressbar(table.get_all(), label="Retrieving rows") as rows:
        table_data = list(rows)

    # Query for census data for each row
    updates = []
    with click.progressbar(table_data, label="Querying for census data") as rows:
        for row in rows:
            if lat_field not in row["fields"]:
                raise Exception(
                    f"Table is missing requred lat field ({lat_field})"
                )

            if lng_field not in row["fields"]:
                raise Exception(
                    f"Table is missing requred lng field ({lng_field})"
                )

            if tract_field not in row["fields"]:
                raise Exception(
                    f"Table is missing requred tract field ({tract_field})"
                )

            if not row["fields"][tract_field]:
                continue

            lat = row["fields"][lat_field]
            lng = row["fields"][lng_field]

            if not lat or not lng:
                click.echo(
                    f"Skipping row {row['id']} because it "
                    "is missing a lat-lng value"
                )

            updates.append({
                "id": row["id"],
                "fields": {tract_field: ""},
            })

    # Verify if we should issue an update
    if not updates:
        click.echo('No rows needed to be updated.')
        return

    if not force:
        click.confirm(f"Apply {len(updates)} updates?", abort=True)

    # Apply the update in bulk
    with click.progressbar(updates, label="Updating rows") as rows:
        table.bulk_update(rows)


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    dotenv.load_dotenv()
    cli()
