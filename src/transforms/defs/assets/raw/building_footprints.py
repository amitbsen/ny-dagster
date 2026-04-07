"""Asset: NYC building footprints loaded from CSV with parsed WKT geometry.

Inputs : BUILDING_20260401.csv (uploads_dir)
Output : building_footprints (DuckDB table)
"""


from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.defs.resources.pipeline_paths import PipelinePaths
from transforms.lib.duckdb_rel import from_csv, write_table
from transforms.logic.transforms.cast_columns import cast_columns
from transforms.logic.transforms.filter_non_null_geometry import (
    filter_non_null_geometry,
)
from transforms.logic.transforms.parse_wkt_geometry import parse_wkt_geometry
from transforms.logic.transforms.rename_columns import rename_columns
from transforms.schemas.column_maps import (
    BUILDING_FOOTPRINTS,
    BUILDING_FOOTPRINTS_CASTS,
)

TABLE_NAME = "building_footprints"
CSV_FILENAME = "BUILDING_20260401.csv"


@asset(
    group_name="raw",
    description="NYC building footprints with snake_case columns and parsed WKT geometry.",
)
def building_footprints(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
    duckdb: DuckdbResource,
) -> None:
    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading building footprints from {csv_path}")

    with duckdb.connection() as con:
        rel = (
            from_csv(con, csv_path.as_posix())
            .pipe(rename_columns, columns=BUILDING_FOOTPRINTS)
            .pipe(cast_columns, casts=BUILDING_FOOTPRINTS_CASTS)
            .pipe(parse_wkt_geometry, source_col="the_geom")
            .pipe(filter_non_null_geometry)
        )
        row_count = write_table(con, rel, TABLE_NAME)

    context.log.info(f"Wrote {row_count} building footprints to {TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_buildings": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
