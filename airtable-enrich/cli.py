#!/usr/bin/env python

"""
Entry point for running all data enrichment commands.
"""
import functools
import os
import pathlib
from typing import Optional

import airtable
import click
import dotenv
import geopandas as gpd
import requests
import shapely.geometry


DATA_DIR = pathlib.Path(__file__).parent / "data"
TRACT_SHP = DATA_DIR / "cb_2019_06_tract_500k.zip"


CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
DEFAULT_BENCHMARK = "Public_AR_Current"
DEFAULT_VINTAGE = "Current_Current"


def tract_from_census_geocoder(lat: float, lng: float) -> Optional[str]:
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

        return tract["GEOID"]


# Cache of loaded tracts geodataframe
@functools.cache
def load_tracts() -> gpd.GeoDataFrame:
    return gpd.read_file(TRACT_SHP)


def tract_from_census_shapefile(lat: float, lng: float) -> Optional[str]:
    tracts_gpd = load_tracts()

    lng_lat = shapely.geometry.Point(lng, lat)

    matches = tracts_gpd.sindex.query(lng_lat, 'within')

    if matches.size == 0:
        return None

    return tracts_gpd["GEOID"].iloc[matches[0]]


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
@click.option("--confirm/--no-confirm",  "confirm", type=bool, default=True)
@click.option("--override/--no-override",  "override", type=bool, default=False)
@click.option("--engine", type=str, default="shapefile")
def fill_census(
    basekey: str,
    tablename: str,
    apikey: str,
    lat_field: str,
    lng_field: str,
    tract_field: str,
    limit: Optional[int],
    confirm: bool,
    override: bool,
    engine: str,
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
            lat_str = row["fields"].get(lat_field)
            lng_str = row["fields"].get(lng_field)

            if not lat_str or not lng_str:
                click.echo(
                    f"Skip row {row['id']} because it "
                    "is missing a lat-lng value"
                )
                continue

            lat = float(lat_str)
            lng = float(lng_str)

            if engine == "geocoder":
                tract = tract_from_census_geocoder(lat, lng)
            elif engine == "shapefile":
                tract = tract_from_census_shapefile(lat, lng)
            else:
                raise Exception(f"Invalid engine specified {engine}")

            if not tract:
                click.echo(
                    f"Skip row {row['id']} because "
                    f"geocoder didn't return a tract for ({lat}, {lng})"
                )
                continue

            # Strip the leading zero off the geoid because that
            # is the format used by HPI :(
            geoid = str(int(tract))

            existing_geoid = row["fields"].get(tract_field)
            if existing_geoid == geoid:
                # Skip updating value because it is the same
                continue

            elif existing_geoid and not override:
                click.echo(
                    f"Skip row {row['id']} because "
                    f"existing value ({existing_geoid}) is different "
                    f"than new value ({geoid})"
                )
                continue

            updates.append({
                "id": row["id"],
                "fields": {tract_field: geoid},
            })

    # Verify if we should issue an update
    if not updates:
        click.echo('No rows needed to be updated.')
        return

    example_value = updates[0]['fields'][tract_field]

    if confirm:
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
