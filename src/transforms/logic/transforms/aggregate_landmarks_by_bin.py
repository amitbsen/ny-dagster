"""Aggregate the ``landmarks`` table into one row per BIN."""

from __future__ import annotations

from duckdb import DuckDBPyConnection, DuckDBPyRelation

# Values that should NOT be aggregated into landmark metadata because they
# represent missing-data sentinels in the source CSV.
_NOISE_VALUES = ("0", "Not determined")


def aggregate_landmarks_by_bin(
    con: DuckDBPyConnection,
    *,
    source_table: str = "landmarks",
) -> DuckDBPyRelation:
    """Return one landmark-summary row per BIN from ``source_table``.

    The returned relation has the columns: ``bin``, ``is_landmark``,
    ``historic_districts``, ``styles``, ``architects``, ``materials``,
    ``original_uses``, ``is_individual_landmark``, ``landmark_entry_count``.
    """
    noise_list = ", ".join(f"'{v}'" for v in _NOISE_VALUES)

    def _agg(col: str) -> str:
        return (
            f"STRING_AGG(DISTINCT {col}, '; ') "
            f"FILTER (WHERE {col} IS NOT NULL AND TRIM({col}) != '' "
            f"AND {col} NOT IN ({noise_list}))"
        )

    sql = f"""
        SELECT
            bin,
            TRUE AS is_landmark,
            {_agg("historic_district")} AS historic_districts,
            {_agg("style_primary")} AS styles,
            {_agg("architect_builder")} AS architects,
            {_agg("material_primary")} AS materials,
            {_agg("use_original")} AS original_uses,
            BOOL_OR(landmark_original != '0' AND landmark_original != '')
                AS is_individual_landmark,
            COUNT(*) AS landmark_entry_count
        FROM {source_table}
        WHERE bin IS NOT NULL AND TRIM(bin) != ''
        GROUP BY bin
    """
    return con.sql(sql)
