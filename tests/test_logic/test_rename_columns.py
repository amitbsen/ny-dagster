"""Tests for rename_columns."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.rename_columns import rename_columns


def test_rename_columns_projects_and_renames(con: DuckDBPyConnection) -> None:
    rel = con.sql("SELECT 'a' AS \"Full Name\", 'b' AS \"Addr:Street\", 'c' AS unused")
    renamed = rel.pipe(
        rename_columns,
        columns={"Full Name": "full_name", "Addr:Street": "addr_street"},
    )
    assert renamed.columns == ["full_name", "addr_street"]
    row = renamed.fetchone()
    assert row == ("a", "b")


def test_rename_columns_preserves_order(con: DuckDBPyConnection) -> None:
    rel = con.sql("SELECT 1 AS x, 2 AS y, 3 AS z")
    renamed = rel.pipe(rename_columns, columns={"z": "zed", "x": "ex"})
    assert renamed.columns == ["zed", "ex"]
