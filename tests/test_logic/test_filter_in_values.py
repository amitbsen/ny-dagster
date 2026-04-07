"""Tests for filter_in_values."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.filter_in_values import filter_in_values


def test_keeps_only_whitelisted(con: DuckDBPyConnection) -> None:
    rel = con.sql(
        "SELECT 'a' AS cat "
        "UNION ALL SELECT 'b' "
        "UNION ALL SELECT 'c'"
    )
    out = rel.pipe(filter_in_values, column="cat", values=["a", "c"])
    rows = sorted(r[0] for r in out.fetchall())
    assert rows == ["a", "c"]
