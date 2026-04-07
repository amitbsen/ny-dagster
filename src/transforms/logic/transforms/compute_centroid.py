"""Add a centroid column computed from a geometry column."""

from __future__ import annotations

from duckdb import DuckDBPyRelation


def compute_centroid(
    rel: DuckDBPyRelation,
    *,
    geometry_col: str = "geometry",
    target_col: str = "centroid",
) -> DuckDBPyRelation:
    """Append a centroid column derived from ``geometry_col``."""
    return rel.project(f"*, ST_Centroid({geometry_col}) AS {target_col}")
