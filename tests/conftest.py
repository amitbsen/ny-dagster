"""Shared pytest fixtures.

Every test that touches DuckDB uses the ``con`` fixture — an in-memory
connection with the spatial extension loaded and the ``.pipe()`` monkey
patch applied via importing ``transforms.lib.duckdb_rel``.
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest
from duckdb import DuckDBPyConnection

# Side effect: attaches .pipe() to DuckDBPyRelation.
import transforms.lib.duckdb_rel  # noqa: F401


@pytest.fixture
def con() -> Iterator[DuckDBPyConnection]:
    """In-memory DuckDB connection with the spatial extension loaded."""
    conn = duckdb.connect(":memory:")
    conn.install_extension("spatial")
    conn.load_extension("spatial")
    try:
        yield conn
    finally:
        conn.close()
