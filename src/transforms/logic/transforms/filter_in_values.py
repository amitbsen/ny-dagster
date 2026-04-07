"""Filter a relation to rows where a column is in a whitelist of values."""

from __future__ import annotations

from collections.abc import Iterable

from duckdb import DuckDBPyRelation


def filter_in_values(
    rel: DuckDBPyRelation,
    *,
    column: str,
    values: Iterable[str],
) -> DuckDBPyRelation:
    """Keep rows where ``column`` is in ``values``."""
    escaped = ", ".join(f"'{v}'" for v in values)
    return rel.filter(f"{column} IN ({escaped})")
