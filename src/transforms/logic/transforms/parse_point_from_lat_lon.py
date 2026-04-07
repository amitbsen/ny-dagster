"""Construct a POINT geometry from lat/lon VARCHAR columns."""

from __future__ import annotations

from duckdb import DuckDBPyRelation


def parse_point_from_lat_lon(
    rel: DuckDBPyRelation,
    *,
    lat_col: str,
    lon_col: str,
    target_col: str = "geometry",
) -> DuckDBPyRelation:
    """Add a POINT geometry column from VARCHAR latitude/longitude columns.

    Rows with missing or blank coordinates get NULL geometry and are
    retained. The lat/lon columns are preserved in the output.

    Args:
        rel: Input relation containing ``lat_col`` and ``lon_col`` as
            strings or numerics.
        lat_col: Latitude column name.
        lon_col: Longitude column name.
        target_col: Name of the resulting geometry column.
    """
    expr = (
        f"CASE WHEN \"{lat_col}\" IS NOT NULL AND TRIM(CAST(\"{lat_col}\" AS VARCHAR)) != '' "
        f"AND \"{lon_col}\" IS NOT NULL AND TRIM(CAST(\"{lon_col}\" AS VARCHAR)) != '' "
        f"THEN ST_Point(CAST(\"{lon_col}\" AS DOUBLE), CAST(\"{lat_col}\" AS DOUBLE)) "
        f"ELSE NULL END AS {target_col}"
    )
    return rel.project(f"*, {expr}")
