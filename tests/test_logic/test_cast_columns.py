"""Tests for cast_columns."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.cast_columns import cast_columns


def test_cast_numeric_strips_commas_and_handles_blanks(
    con: DuckDBPyConnection,
) -> None:
    rel = con.sql(
        "SELECT '1,234.5' AS area UNION ALL SELECT '' UNION ALL SELECT NULL"
    )
    casted = rel.pipe(cast_columns, casts={"area": "DOUBLE"})
    rows = [r[0] for r in casted.fetchall()]
    assert rows[0] == 1234.5
    assert rows[1] is None
    assert rows[2] is None


def test_cast_preserves_other_columns(con: DuckDBPyConnection) -> None:
    rel = con.sql("SELECT 'foo' AS name, '42' AS n")
    casted = rel.pipe(cast_columns, casts={"n": "INTEGER"})
    assert casted.columns == ["name", "n"]
    row = casted.fetchone()
    assert row == ("foo", 42)


def test_cast_non_numeric_type_applies_plain_cast(con: DuckDBPyConnection) -> None:
    rel = con.sql("SELECT 42 AS n")
    casted = rel.pipe(cast_columns, casts={"n": "VARCHAR"})
    assert casted.fetchone() == ("42",)
