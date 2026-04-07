"""Asset: NYC individual landmark and historic district buildings.

Inputs : Individual_Landmark_and_Historic_District_Building_Database_20260401.csv
Output : landmarks (DuckDB table)
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
from transforms.schemas.column_maps import LANDMARKS, LANDMARKS_CASTS

TABLE_NAME = "landmarks"
CSV_FILENAME = (
    "Individual_Landmark_and_Historic_District_Building_Database_20260401.csv"
)


@asset(
    group_name="raw",
    description="NYC landmarks and historic district buildings with parsed geometry.",
)
def landmarks(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
    duckdb: DuckdbResource,
) -> None:
    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading landmarks from {csv_path}")

    with duckdb.connection() as con:
        rel = (
            from_csv(con, csv_path.as_posix())
            .pipe(rename_columns, columns=LANDMARKS)
            .pipe(cast_columns, casts=LANDMARKS_CASTS)
            .pipe(parse_wkt_geometry, source_col="the_geom")
            .pipe(filter_non_null_geometry)
        )
        row_count = write_table(con, rel, TABLE_NAME)

    context.log.info(f"Wrote {row_count} landmarks to {TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_landmarks": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
