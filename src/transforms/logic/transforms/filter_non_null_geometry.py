"""Drop rows with NULL geometry."""

from __future__ import annotations

from duckdb import DuckDBPyRelation


def filter_non_null_geometry(
    rel: DuckDBPyRelation,
    *,
    geometry_col: str = "geometry",
) -> DuckDBPyRelation:
    """Keep only rows where the geometry column is not NULL."""
    return rel.filter(f"{geometry_col} IS NOT NULL")
