"""Tests for filter_non_null_geometry."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.filter_non_null_geometry import (
    filter_non_null_geometry,
)


def test_drops_null_rows(con: DuckDBPyConnection) -> None:
    rel = con.sql(
        "SELECT ST_GeomFromText('POINT(0 0)') AS geometry "
        "UNION ALL SELECT NULL"
    )
    out = rel.pipe(filter_non_null_geometry)
    row = out.count("*").fetchone()
    assert row == (1,)
