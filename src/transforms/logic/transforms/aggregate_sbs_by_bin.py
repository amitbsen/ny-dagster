"""Aggregate SBS certified businesses into one row per BIN."""

from __future__ import annotations

from duckdb import DuckDBPyConnection, DuckDBPyRelation


def aggregate_sbs_by_bin(
    con: DuckDBPyConnection,
    *,
    source_table: str = "sbs_certified_businesses",
) -> DuckDBPyRelation:
    """Return one SBS-summary row per BIN from ``source_table``."""

    def _agg(col: str) -> str:
        return (
            f"STRING_AGG(DISTINCT {col}, '; ') "
            f"FILTER (WHERE {col} IS NOT NULL AND TRIM({col}) != '')"
        )

    sql = f"""
        SELECT
            bin,
            TRUE AS has_sbs_business,
            COUNT(*) AS sbs_business_count,
            {_agg("certification")} AS sbs_certification_types,
            {_agg("vendor_formal_name")} AS sbs_business_names,
            {_agg("ethnicity")} AS sbs_ethnicities,
            {_agg("naics_sector")} AS sbs_naics_sectors
        FROM {source_table}
        WHERE bin IS NOT NULL AND TRIM(bin) != ''
        GROUP BY bin
    """
    return con.sql(sql)
