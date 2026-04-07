"""Tests for the write_table schema-assertion path."""

from __future__ import annotations

import pytest
from duckdb import DuckDBPyConnection

from transforms.lib.duckdb_rel import SchemaMismatchError, write_table


def test_write_table_succeeds_when_schema_matches(con: DuckDBPyConnection) -> None:
    rel = con.sql("SELECT 'foo' AS name, 42 AS age")
    count = write_table(
        con,
        rel,
        "people",
        schema={"name": "VARCHAR", "age": "INTEGER"},
    )
    assert count == 1


def test_write_table_raises_on_missing_column(con: DuckDBPyConnection) -> None:
    rel = con.sql("SELECT 'foo' AS name")
    with pytest.raises(SchemaMismatchError):
        write_table(
            con,
            rel,
            "people",
            schema={"name": "VARCHAR", "age": "INTEGER"},
        )


def test_write_table_raises_on_type_mismatch(con: DuckDBPyConnection) -> None:
    rel = con.sql("SELECT 'foo' AS name, 'bar' AS age")
    with pytest.raises(SchemaMismatchError):
        write_table(
            con,
            rel,
            "people",
            schema={"name": "VARCHAR", "age": "INTEGER"},
        )
