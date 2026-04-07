"""Tests for parse_point_from_lat_lon."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.parse_point_from_lat_lon import (
    parse_point_from_lat_lon,
)


def test_builds_point_and_preserves_coords(con: DuckDBPyConnection) -> None:
    rel = con.sql("SELECT '40.7' AS lat, '-74.0' AS lon")
    out = rel.pipe(parse_point_from_lat_lon, lat_col="lat", lon_col="lon")
    row = out.project(
        "ST_AsText(geometry) AS wkt, lat, lon"
    ).fetchone()
    assert row is not None
    assert row[0] == "POINT (-74 40.7)"


def test_blank_coords_yield_null(con: DuckDBPyConnection) -> None:
    rel = con.sql(
        "SELECT '40.7' AS lat, '' AS lon "
        "UNION ALL SELECT NULL, '-74.0'"
    )
    out = rel.pipe(parse_point_from_lat_lon, lat_col="lat", lon_col="lon")
    row = out.filter("geometry IS NULL").count("*").fetchone()
    assert row is not None
    assert row[0] == 2
