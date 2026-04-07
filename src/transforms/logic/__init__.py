"""Pure, composable transform and expression functions.

Nothing in this package imports from Dagster or opens database connections.
Transforms in ``logic.transforms`` take and return ``DuckDBPyRelation``.
Expressions in ``logic.expressions`` return SQL expression strings or
SELECT fragments that can be embedded in larger queries.
"""
