"""Identify buildings that spatially contain at least one business."""

from __future__ import annotations

from duckdb import DuckDBPyConnection, DuckDBPyRelation


def find_buildings_containing_businesses(
    con: DuckDBPyConnection,
    *,
    buildings_table: str = "building_footprints",
    businesses_table: str = "derived_businesses",
) -> DuckDBPyRelation:
    """Return the distinct set of BINs that contain any business geometry."""
    return con.sql(
        f"SELECT DISTINCT b.bin "
        f"FROM {buildings_table} b "
        f"INNER JOIN {businesses_table} biz "
        f"ON ST_Within(biz.geometry, b.geometry)"
    )
