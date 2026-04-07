"""Convert arbitrary column names into snake_case."""

from __future__ import annotations

import re

_CAMEL_BOUNDARY_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_BOUNDARY_2 = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")


def to_snake_case(name: str) -> str:
    """Convert an arbitrary string to snake_case.

    Handles CamelCase, spaces, punctuation, and mixed separators. Multiple
    separators collapse into a single underscore and the result is stripped
    of leading/trailing underscores.

    >>> to_snake_case("Building Name")
    'building_name'
    >>> to_snake_case("BINNumber")
    'bin_number'
    >>> to_snake_case("addr:street")
    'addr_street'
    """
    # Insert underscore at camelCase / PascalCase boundaries.
    s = _CAMEL_BOUNDARY_1.sub(r"\1_\2", name)
    s = _CAMEL_BOUNDARY_2.sub(r"\1_\2", s)
    # Replace every non-alphanumeric run with a single underscore.
    s = _NON_ALNUM.sub("_", s)
    return s.strip("_").lower()
