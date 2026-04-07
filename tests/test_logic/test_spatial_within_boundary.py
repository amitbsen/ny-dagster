"""Tests for spatial_within_boundary."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.spatial_within_boundary import (
    spatial_within_boundary,
)


def test_filters_to_points_inside_boundary(con: DuckDBPyConnection) -> None:
    # Set up a boundary temp table with a 10x10 square centered at origin.
    con.execute(
        "CREATE OR REPLACE TEMP TABLE boundary AS "
        "SELECT ST_GeomFromText('POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))') AS geom"
    )
    rel = con.sql(
        "SELECT ST_Point(0, 0) AS geometry, 'inside' AS label "
        "UNION ALL SELECT ST_Point(10, 10), 'outside'"
    )
    out = rel.pipe(spatial_within_boundary)
    row = out.project("label").fetchall()
    assert row == [("inside",)]
