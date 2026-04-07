"""Union the four geocoded business sources into one harmonized relation."""

from __future__ import annotations

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from transforms.logic.expressions import (
    harmonize_dca_columns,
    harmonize_osm_columns,
    harmonize_retail_columns,
    harmonize_sbs_columns,
)


def union_business_sources(
    con: DuckDBPyConnection,
    *,
    boundary_table: str = "boundary",
) -> DuckDBPyRelation:
    """Return a harmonized union of OSM, DCA, retail-food, and SBS sources.

    Every source is spatially filtered to rows within ``boundary_table``
    and projected onto the same column schema. ``boundary_table`` must
    already exist in the connection.
    """
    sql = f"""
        SELECT * FROM (
            {harmonize_osm_columns.SELECT}
            FROM osm_businesses o
            CROSS JOIN {boundary_table} bnd
            WHERE ST_Within(o.geometry, bnd.geom)

            UNION ALL

            {harmonize_dca_columns.SELECT}
            FROM issued_licenses il
            CROSS JOIN {boundary_table} bnd
            WHERE ST_Within(il.geometry, bnd.geom)

            UNION ALL

            {harmonize_retail_columns.SELECT}
            FROM retail_food_stores rfs
            CROSS JOIN {boundary_table} bnd
            WHERE ST_Within(rfs.geometry, bnd.geom)

            UNION ALL

            {harmonize_sbs_columns.SELECT}
            FROM sbs_certified_businesses sbs
            CROSS JOIN {boundary_table} bnd
            WHERE ST_Within(sbs.geometry, bnd.geom)
        ) harmonized
    """
    return con.sql(sql)
