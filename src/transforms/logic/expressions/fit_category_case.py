"""Fit-category tier assignment expressed as a DuckDB CASE.

The expression assigns one of three tiers to a business based on its
harmonized ``subcategory``. Subcategories outside the tier lists get NULL.

The expression contains a ``{source}`` placeholder that callers must
``.format()`` with the column name holding the subcategory.
"""

from __future__ import annotations

TIER_1_HIGH_FIT = ("cafe", "gallery", "books", "florist", "garden")
TIER_2_STRONG_POTENTIAL = ("bakery", "bar", "hotel", "gift", "community_centre")
TIER_3_WORTH_EXPLORING = ("library", "museum", "coworking", "artwork")

ALL_FIT_SUBCATEGORIES: tuple[str, ...] = (
    TIER_1_HIGH_FIT + TIER_2_STRONG_POTENTIAL + TIER_3_WORTH_EXPLORING
)


def _in(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


FIT_CATEGORY_CASE: str = (
    "CASE "
    f"WHEN {{source}} IN ({_in(TIER_1_HIGH_FIT)}) THEN 'tier_1_high_fit' "
    f"WHEN {{source}} IN ({_in(TIER_2_STRONG_POTENTIAL)}) THEN 'tier_2_strong_potential' "
    f"WHEN {{source}} IN ({_in(TIER_3_WORTH_EXPLORING)}) THEN 'tier_3_worth_exploring' "
    "END"
)
