"""Rename columns according to an explicit mapping."""

from __future__ import annotations

from duckdb import DuckDBPyRelation


def rename_columns(
    rel: DuckDBPyRelation,
    *,
    columns: dict[str, str],
) -> DuckDBPyRelation:
    """Rename and project columns using an explicit source->target mapping.

    Only columns present in ``columns`` are kept. The output column order
    follows the insertion order of the mapping. Source column names that
    contain spaces or punctuation are quoted automatically.

    Args:
        rel: Input relation.
        columns: ``{source_column_name: target_column_name}``.

    Returns:
        A projected relation with the renamed columns.
    """
    parts = [f'"{src}" AS {tgt}' for src, tgt in columns.items()]
    return rel.project(", ".join(parts))
