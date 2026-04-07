"""Parse a WKT string column into a DuckDB GEOMETRY column."""

from __future__ import annotations

from duckdb import DuckDBPyRelation


def parse_wkt_geometry(
    rel: DuckDBPyRelation,
    *,
    source_col: str,
    target_col: str = "geometry",
    drop_source: bool = True,
) -> DuckDBPyRelation:
    """Add a GEOMETRY column parsed from a VARCHAR WKT column.

    Empty strings and NULLs produce NULL geometry instead of raising.

    Args:
        rel: Input relation.
        source_col: Name of the WKT column.
        target_col: Name of the resulting GEOMETRY column. Defaults to
            ``"geometry"``.
        drop_source: Whether to drop ``source_col`` from the output.
    """
    geom_expr = (
        f"CASE WHEN \"{source_col}\" IS NULL OR TRIM(\"{source_col}\") = '' "
        f"THEN NULL ELSE ST_GeomFromText(\"{source_col}\") END AS {target_col}"
    )
    if drop_source:
        return rel.project(f"* EXCLUDE (\"{source_col}\"), {geom_expr}")
    return rel.project(f"*, {geom_expr}")
