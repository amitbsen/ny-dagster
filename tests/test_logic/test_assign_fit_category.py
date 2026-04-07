"""Tests for assign_fit_category."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.assign_fit_category import assign_fit_category


def test_assigns_expected_tiers(con: DuckDBPyConnection) -> None:
    rel = con.sql(
        "SELECT 'cafe' AS subcategory "
        "UNION ALL SELECT 'bakery' "
        "UNION ALL SELECT 'library' "
        "UNION ALL SELECT 'pharmacy'"
    )
    out = rel.pipe(assign_fit_category)
    rows = {r[0]: r[1] for r in out.project("subcategory, fit_category").fetchall()}
    assert rows["cafe"] == "tier_1_high_fit"
    assert rows["bakery"] == "tier_2_strong_potential"
    assert rows["library"] == "tier_3_worth_exploring"
    assert rows["pharmacy"] is None
