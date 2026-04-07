"""Asset: NYC SBS certified business list with point geometry.

Inputs : SBS_Certified_Business_List_20260401.csv
Output : sbs_certified_businesses (DuckDB table)
"""


from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.defs.resources.pipeline_paths import PipelinePaths
from transforms.lib.duckdb_rel import from_csv, write_table
from transforms.logic.transforms.cast_columns import cast_columns
from transforms.logic.transforms.parse_point_from_lat_lon import (
    parse_point_from_lat_lon,
)
from transforms.logic.transforms.rename_columns import rename_columns
from transforms.schemas.column_maps import SBS_CASTS, SBS_CERTIFIED_BUSINESSES

TABLE_NAME = "sbs_certified_businesses"
CSV_FILENAME = "SBS_Certified_Business_List_20260401.csv"


@asset(
    group_name="raw",
    description="NYC SBS certified businesses with point geometry constructed from lat/lon.",
)
def sbs_certified_businesses(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
    duckdb: DuckdbResource,
) -> None:
    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading SBS certified businesses from {csv_path}")

    with duckdb.connection() as con:
        rel = (
            from_csv(con, csv_path.as_posix())
            .pipe(rename_columns, columns=SBS_CERTIFIED_BUSINESSES)
            .pipe(parse_point_from_lat_lon, lat_col="latitude", lon_col="longitude")
            .pipe(cast_columns, casts=SBS_CASTS)
        )
        row_count = write_table(con, rel, TABLE_NAME)

    context.log.info(f"Wrote {row_count} SBS businesses to {TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_businesses": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
