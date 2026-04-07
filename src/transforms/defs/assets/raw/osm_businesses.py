"""Asset: OpenStreetMap businesses and amenities in NY State.

Inputs : Overpass API (NY bounding box)
Output : osm_businesses (DuckDB table)
"""


import json
import os
import tempfile

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.lib.duckdb_rel import write_table
from transforms.logic.transforms.parse_point_from_lat_lon import (
    parse_point_from_lat_lon,
)
from transforms.utils.overpass_client import extract_rows, fetch_elements

TABLE_NAME = "osm_businesses"
NY_BBOX = "40.477,-79.763,45.016,-71.856"


@asset(
    group_name="raw",
    description="Businesses and amenities in NY State from OpenStreetMap via Overpass API.",
)
def osm_businesses(
    context: AssetExecutionContext,
    duckdb: DuckdbResource,
) -> None:
    context.log.info(f"Querying Overpass API with bbox {NY_BBOX}")
    elements = fetch_elements(NY_BBOX)
    context.log.info(f"Received {len(elements)} elements from Overpass")
    rows = extract_rows(elements)
    context.log.info(f"Extracted {len(rows)} elements with coordinates")

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as tmp:
        tmp_path = tmp.name
        json.dump(rows, tmp)

    try:
        with duckdb.connection() as con:
            raw = con.sql(f"SELECT * FROM read_json_auto('{tmp_path}')")
            rel = raw.pipe(
                parse_point_from_lat_lon, lat_col="lat", lon_col="lon"
            )
            # Promote lat/lon to DOUBLE and osm_id to BIGINT after geometry add.
            rel = rel.project(
                "geometry, osm_type, CAST(osm_id AS BIGINT) AS osm_id, name, "
                "amenity, shop, tourism, craft, office, leisure, cuisine, "
                "brand, opening_hours, phone, website, addr_street, "
                "addr_housenumber, addr_city, addr_postcode, other_tags, "
                "CAST(lat AS DOUBLE) AS latitude, CAST(lon AS DOUBLE) AS longitude"
            )
            row_count = write_table(con, rel, TABLE_NAME)

            named_row = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE name IS NOT NULL"
            ).fetchone()
            categories: dict[str, int] = {}
            for tag in ("amenity", "shop", "tourism", "craft", "office", "leisure"):
                result = con.execute(
                    f"SELECT count(*) FROM {TABLE_NAME} WHERE {tag} IS NOT NULL"
                ).fetchone()
                if result and result[0]:
                    categories[tag] = int(result[0])
    finally:
        os.unlink(tmp_path)

    named = int(named_row[0]) if named_row else 0
    context.log.info(f"Wrote {row_count} OSM businesses ({named} named)")
    context.add_output_metadata(
        {
            "num_elements": MetadataValue.int(row_count),
            "num_named": MetadataValue.int(named),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            "bbox": MetadataValue.text(NY_BBOX),
            **{f"tag_{k}": MetadataValue.int(v) for k, v in categories.items()},
        }
    )
