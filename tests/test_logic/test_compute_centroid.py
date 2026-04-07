"""Tests for compute_centroid."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.compute_centroid import compute_centroid


def test_centroid_of_unit_square(con: DuckDBPyConnection) -> None:
    rel = con.sql(
        "SELECT ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS geometry"
    )
    out = rel.pipe(compute_centroid)
    assert "centroid" in out.columns
    row = out.project("ST_AsText(centroid)").fetchone()
    assert row is not None
    assert row[0] == "POINT (0.5 0.5)"
