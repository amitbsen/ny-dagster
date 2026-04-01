"""Resource that provides file paths for the pipeline."""

from pathlib import Path

from dagster import ConfigurableResource


class PipelinePaths(ConfigurableResource):
    """Paths to uploads and database used by the pipeline.

    Args:
        uploads_dir: Directory containing uploaded source CSV files.
        duckdb_path: Path to the DuckDB database file for storage.
    """

    uploads_dir: str
    duckdb_path: str

    @property
    def uploads_path(self) -> Path:
        """Return the uploads directory as a Path."""
        return Path(self.uploads_dir)

    @property
    def db_path(self) -> Path:
        """Return the DuckDB database file path."""
        return Path(self.duckdb_path)
