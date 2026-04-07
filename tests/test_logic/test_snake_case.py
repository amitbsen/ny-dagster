"""Tests for the snake_case utility."""

from __future__ import annotations

import pytest

from transforms.utils.snake_case import to_snake_case


@pytest.mark.parametrize(
    "source,expected",
    [
        ("Building Name", "building_name"),
        ("BINNumber", "bin_number"),
        ("addr:street", "addr_street"),
        ("ID6_digit_NAICS_code", "id6_digit_naics_code"),
        ("Census Tract (2020)", "census_tract_2020"),
        ("already_snake", "already_snake"),
        ("Multiple   Spaces", "multiple_spaces"),
    ],
)
def test_snake_case(source: str, expected: str) -> None:
    assert to_snake_case(source) == expected
