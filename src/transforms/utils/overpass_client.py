"""Client for the OpenStreetMap Overpass API.

This module is pure: no Dagster, no DuckDB. Assets import ``fetch_elements``
and ``extract_rows`` and are responsible for writing the result to storage.
"""

from __future__ import annotations

import json
from typing import Any

import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Tag keys to extract as dedicated columns when flattening Overpass elements.
TAG_COLUMNS: list[str] = [
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


def build_query(bbox: str, *, timeout: int = 300) -> str:
    """Build an Overpass QL query for businesses/amenities in a bounding box.

    Args:
        bbox: Comma-separated ``south,west,north,east``.
        timeout: Overpass timeout in seconds.

    Returns:
        The Overpass QL query string.
    """
    return (
        f"[out:json][timeout:{timeout}];\n"
        "(\n"
        f'  nwr["amenity"]({bbox});\n'
        f'  nwr["shop"]({bbox});\n'
        f'  nwr["tourism"]({bbox});\n'
        f'  nwr["craft"]({bbox});\n'
        f'  nwr["office"]({bbox});\n'
        f'  nwr["leisure"]({bbox});\n'
        ");\n"
        "out center;"
    )


def fetch_elements(bbox: str, *, request_timeout: int = 360) -> list[dict[str, Any]]:
    """Query the Overpass API and return the raw element list."""
    response = requests.post(
        OVERPASS_URL,
        data={"data": build_query(bbox)},
        timeout=request_timeout,
    )
    response.raise_for_status()
    return list(response.json().get("elements", []))


def extract_row(element: dict[str, Any]) -> dict[str, Any] | None:
    """Flatten a single Overpass element into a row dict.

    Nodes use their direct lat/lon. Ways and relations use the ``center``
    computed by the ``out center`` directive. Elements without coordinates
    return ``None``.
    """
    osm_type = element.get("type", "")
    osm_id = element.get("id")
    tags = element.get("tags", {})

    if osm_type == "node":
        lat = element.get("lat")
        lon = element.get("lon")
    else:
        center = element.get("center", {})
        lat = center.get("lat")
        lon = center.get("lon")

    if lat is None or lon is None:
        return None

    row: dict[str, Any] = {
        "osm_type": osm_type,
        "osm_id": osm_id,
        "lat": lat,
        "lon": lon,
    }
    for key in TAG_COLUMNS:
        row[key.replace(":", "_")] = tags.get(key)

    remaining = {k: v for k, v in tags.items() if k not in TAG_COLUMNS}
    row["other_tags"] = json.dumps(remaining) if remaining else None
    return row


def extract_rows(elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten all elements, dropping those without coordinates."""
    rows = []
    for element in elements:
        row = extract_row(element)
        if row is not None:
            rows.append(row)
    return rows
