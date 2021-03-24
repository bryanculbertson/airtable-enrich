#!/usr/bin/env python

"""
Entry point for running all data enrichment commands.
"""
import os
from typing import Optional

import airtable
import click
import dotenv
import requests


CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
DEFAULT_BENCHMARK = "Public_AR_Current"
DEFAULT_VINTAGE = "Current_Current"


def tract_for_latlng(lat: float, lng: float) -> Optional[dict]:
    params = {
        "x": lng,
        "y": lat,
        "format": "json",
        "vintage": DEFAULT_VINTAGE,
        "benchmark": DEFAULT_BENCHMARK,
    }

    with requests.get(CENSUS_GEOCODER_URL, params=params, timeout=20) as r:
        response = r.json()

        result = response.get("result")
        if not result:
            return None

        geographies = result.get("geographies")

        tracts = geographies.get("Census Tracts")
        if not tracts:
            return None

        tract = tracts[0]
        if tract.get("status"):
            return None

        return tract

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
@click.option("--limit", type=int)
@click.option("--force", type=bool, default=False)
def fill_census(
    basekey: str,
    tablename: str,
    apikey: str,
    lat_field: str,
    lng_field: str,
    tract_field: str,
    limit: Optional[int],
    force: bool,
):
    """Print the head of the table"""
    table = airtable.Airtable(basekey, tablename, apikey)

    # Download all the rows locally because
    # we are going to modify them in bulk.
    with click.progressbar(
        table.get_all(maxRecords=limit), label="Retrieving rows"
    ) as rows:
        table_data = list(rows)

    # Query for census data for each row
    updates = []
    with click.progressbar(
        table_data, label="Querying for census data"
    ) as rows:
        for row in rows:
            lat = row["fields"].get(lat_field)
            lng = row["fields"].get(lng_field)

            if not lat or not lng:
                click.echo(
                    f"Skipping row {row['id']} because it "
                    "is missing a lat-lng value"
                )

            tract = tract_for_latlng(float(lat), float(lng))

            if not tract:
                click.echo(
                    f"Skipping row {row['id']} because "
                    f"geocoder didn't return a tract for ({lat}, {lng})"
                )
                continue

            # Strip the leading zero off the geoid because that
            # is the format used by HPI :(
            geoid = str(int(tract["GEOID"]))

            updates.append({
                "id": row["id"],
                "fields": {tract_field: geoid},
            })

    # Verify if we should issue an update
    if not updates:
        click.echo('No rows needed to be updated.')
        return

    example_value = updates[0]['fields'][tract_field]

    if not force:
        click.confirm(
            f"Apply {len(updates)} updates "
            f"e.g. {tract_field}={example_value}?",
            abort=True
        )
    else:
        click.echo(
            f"Appling {len(updates)} updates "
            f"e.g. {tract_field}={example_value}?"
        )

    # Apply the update in bulk
    table.batch_update(updates)


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    dotenv.load_dotenv()
    cli()
