"""Asset that fetches businesses and amenities from OpenStreetMap via Overpass API."""

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "osm_businesses"

# New York State bounding box: south, west, north, east
NY_BBOX = "40.477,-79.763,45.016,-71.856"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

OVERPASS_QUERY = f"""[out:json][timeout:300];
(
  nwr["amenity"]({NY_BBOX});
  nwr["shop"]({NY_BBOX});
  nwr["tourism"]({NY_BBOX});
  nwr["craft"]({NY_BBOX});
  nwr["office"]({NY_BBOX});
  nwr["leisure"]({NY_BBOX});
);
out center;"""

# Tag keys to extract as dedicated columns
TAG_COLUMNS = [
    "name",
    "amenity",
    "shop",
    "tourism",
    "craft",
    "office",
    "leisure",
    "cuisine",
    "brand",
    "opening_hours",
    "phone",
    "website",
    "addr:street",
    "addr:housenumber",
    "addr:city",
    "addr:postcode",
]


def _extract_element(element: dict) -> dict | None:
    """Extract a flat row dict from an Overpass JSON element.

    Args:
        element: A single element from the Overpass response.

    Returns:
        A flat dict suitable for DuckDB insertion, or None if no location.
    """
    import json

    osm_type = element.get("type", "")
    osm_id = element.get("id")
    tags = element.get("tags", {})

    # Nodes have lat/lon directly; ways/relations use center
    if osm_type == "node":
        lat = element.get("lat")
        lon = element.get("lon")
    else:
        center = element.get("center", {})
        lat = center.get("lat")
        lon = center.get("lon")

    if lat is None or lon is None:
        return None

    row = {
        "osm_type": osm_type,
        "osm_id": osm_id,
        "lat": lat,
        "lon": lon,
    }

    for key in TAG_COLUMNS:
        row[key.replace(":", "_")] = tags.get(key)

    # Store remaining tags as JSON for future use
    remaining = {k: v for k, v in tags.items() if k not in TAG_COLUMNS}
    row["other_tags"] = json.dumps(remaining) if remaining else None

    return row


@asset(
    group_name="raw",
    description="Businesses and amenities in NY State from OpenStreetMap via Overpass API.",
)
def osm_businesses(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Query the Overpass API for all businesses/amenities in NY State bounding box.

    Fetches nodes, ways, and relations tagged with amenity, shop, tourism,
    craft, office, or leisure. Stores results with point geometry in DuckDB.

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing database path.
    """
    import json
    import tempfile

    import duckdb
    import requests

    context.log.info(f"Querying Overpass API with bbox {NY_BBOX}")
    response = requests.post(
        OVERPASS_URL,
        data={"data": OVERPASS_QUERY},
        timeout=360,
    )
    response.raise_for_status()

    data = response.json()
    elements = data.get("elements", [])
    context.log.info(f"Received {len(elements)} elements from Overpass API")

    rows = []
    for element in elements:
        row = _extract_element(element)
        if row is not None:
            rows.append(row)

    context.log.info(f"Extracted {len(rows)} elements with coordinates")

    # Write to temp NDJSON file to avoid DuckDB string length limits
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    )
    tmp_path = tmp.name
    json.dump(rows, tmp)
    tmp.close()
    context.log.info(f"Wrote temp JSON to {tmp_path}")

    db_path = pipeline_paths.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.install_extension("spatial")
    con.load_extension("spatial")

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            ST_Point(lon, lat) AS geometry,
            osm_type,
            CAST(osm_id AS BIGINT) AS osm_id,
            name,
            amenity,
            shop,
            tourism,
            craft,
            office,
            leisure,
            cuisine,
            brand,
            opening_hours,
            phone,
            website,
            addr_street,
            addr_housenumber,
            addr_city,
            addr_postcode,
            other_tags,
            CAST(lat AS DOUBLE) AS latitude,
            CAST(lon AS DOUBLE) AS longitude
        FROM read_json_auto('{tmp_path}')
    """)

    import os
    os.unlink(tmp_path)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    named_count = con.execute(
        f"SELECT count(*) FROM {TABLE_NAME} WHERE name IS NOT NULL"
    ).fetchone()[0]

    # Category breakdown
    categories = {}
    for tag in ["amenity", "shop", "tourism", "craft", "office", "leisure"]:
        cnt = con.execute(
            f"SELECT count(*) FROM {TABLE_NAME} WHERE {tag} IS NOT NULL"
        ).fetchone()[0]
        if cnt > 0:
            categories[tag] = cnt

    con.close()

    context.log.info(
        f"Wrote {row_count} OSM businesses to {db_path}:{TABLE_NAME} "
        f"({named_count} named)"
    )
    context.add_output_metadata(
        {
            "num_elements": MetadataValue.int(row_count),
            "num_named": MetadataValue.int(named_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(db_path)),
            "bbox": MetadataValue.text(NY_BBOX),
            **{
                f"tag_{k}": MetadataValue.int(v)
                for k, v in categories.items()
            },
        }
    )
