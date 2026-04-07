"""Filter a relation to rows whose geometry is within a boundary polygon."""

from __future__ import annotations

from duckdb import DuckDBPyConnection, DuckDBPyRelation


def load_boundary(
    con: DuckDBPyConnection,
    *,
    geojson_path: str,
    temp_table: str = "boundary",
) -> None:
    """Load a single-polygon GeoJSON into a temp table named ``temp_table``.

    The resulting temp table has one column, ``geom``, of type GEOMETRY.
    This is a side-effecting helper intended to be called once before a
    series of ``spatial_within_boundary`` pipe steps that reference the
    same temp table.
    """
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {temp_table} AS "
        f"SELECT geom FROM ST_Read('{geojson_path}')"
    )


def spatial_within_boundary(
    rel: DuckDBPyRelation,
    *,
    boundary_table: str = "boundary",
    geometry_col: str = "geometry",
) -> DuckDBPyRelation:
    """Keep only rows whose geometry is within any polygon in ``boundary_table``.

    ``boundary_table`` must already exist in the same connection; see
    :func:`load_boundary`.
    """
    return rel.filter(
        f"EXISTS (SELECT 1 FROM {boundary_table} b "
        f"WHERE ST_Within({geometry_col}, b.geom))"
    )
