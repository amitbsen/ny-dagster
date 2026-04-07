"""Cast columns to specific DuckDB types, tolerating commas and blanks."""

from __future__ import annotations

from duckdb import DuckDBPyRelation


def cast_columns(
    rel: DuckDBPyRelation,
    *,
    casts: dict[str, str],
) -> DuckDBPyRelation:
    """Cast the named columns to the given DuckDB types.

    Each target type is applied as ``CAST(REPLACE(col, ',', '') AS type)``
    when the type is numeric so that values like ``"1,234.56"`` survive.
    Blank strings become NULL.

    Args:
        rel: Input relation.
        casts: ``{column_name: duckdb_type}`` (e.g. ``{"area": "DOUBLE"}``).

    Returns:
        Relation with casts applied; all other columns pass through.
    """
    existing = rel.columns
    pieces: list[str] = []
    numeric_types = {"DOUBLE", "INTEGER", "BIGINT", "FLOAT", "DECIMAL"}
    for col in existing:
        if col in casts:
            target_type = casts[col]
            root = target_type.split("(")[0].strip().upper()
            if root in numeric_types:
                pieces.append(
                    f"CASE WHEN \"{col}\" IS NULL OR TRIM(\"{col}\") = '' THEN NULL "
                    f"ELSE CAST(REPLACE(\"{col}\", ',', '') AS {target_type}) END "
                    f"AS {col}"
                )
            else:
                pieces.append(f'CAST("{col}" AS {target_type}) AS {col}')
        else:
            pieces.append(f'"{col}"')
    return rel.project(", ".join(pieces))
