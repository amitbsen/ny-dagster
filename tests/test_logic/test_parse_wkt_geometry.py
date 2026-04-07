"""Tests for parse_wkt_geometry."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.parse_wkt_geometry import parse_wkt_geometry


def test_parses_wkt_and_drops_source(con: DuckDBPyConnection) -> None:
    rel = con.sql("SELECT 'POINT(1 2)' AS the_geom, 'foo' AS name")
    parsed = rel.pipe(parse_wkt_geometry, source_col="the_geom")
    assert "the_geom" not in parsed.columns
    assert "geometry" in parsed.columns
    wkt_row = parsed.project("ST_AsText(geometry) AS wkt, name").fetchone()
    assert wkt_row == ("POINT (1 2)", "foo")


def test_null_and_blank_wkt_become_null_geometry(con: DuckDBPyConnection) -> None:
    rel = con.sql(
        "SELECT 'POINT(0 0)' AS g UNION ALL SELECT '' UNION ALL SELECT NULL"
    )
    parsed = rel.pipe(parse_wkt_geometry, source_col="g")
    null_count_row = parsed.filter("geometry IS NULL").count("*").fetchone()
    assert null_count_row is not None
    assert null_count_row[0] == 2
