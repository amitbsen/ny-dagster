"""DuckDB connection resource with spatial extension loaded."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
from dagster import ConfigurableResource
from duckdb import DuckDBPyConnection


class DuckdbResource(ConfigurableResource):
    """Manages a DuckDB connection with the spatial extension auto-loaded.

    Assets should always acquire the connection through the ``connection``
    context manager rather than calling ``duckdb.connect`` directly. This
    centralizes extension loading, parent-directory creation, and cleanup.

    Args:
        duckdb_path: Filesystem path to the DuckDB database file.
    """

    duckdb_path: str

    @property
    def db_path(self) -> Path:
        """The database path as a ``Path``."""
        return Path(self.duckdb_path)

    @contextmanager
    def connection(self) -> Iterator[DuckDBPyConnection]:
        """Yield an open DuckDB connection with the spatial extension loaded.

        The parent directory is created on first use. The connection is
        closed on exit even if the caller raises.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(self.db_path))
        try:
            con.install_extension("spatial")
            con.load_extension("spatial")
            yield con
        finally:
            con.close()
