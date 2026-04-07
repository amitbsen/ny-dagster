"""Assign an exhibition fit-category tier based on subcategory."""

from __future__ import annotations

from duckdb import DuckDBPyRelation

from transforms.logic.expressions.fit_category_case import FIT_CATEGORY_CASE


def assign_fit_category(
    rel: DuckDBPyRelation,
    *,
    source_col: str = "subcategory",
    target_col: str = "fit_category",
) -> DuckDBPyRelation:
    """Append a ``fit_category`` column derived from ``source_col``.

    Uses the tier mapping in :data:`fit_category_case.FIT_CATEGORY_CASE`.
    Rows whose subcategory is not in any tier get NULL.
    """
    return rel.project(
        f"*, {FIT_CATEGORY_CASE.format(source=source_col)} AS {target_col}"
    )
