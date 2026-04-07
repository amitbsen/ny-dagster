"""Asset: NYC DCA issued business licenses with point geometry.

Inputs : Issued_Licenses_20260401.csv
Output : issued_licenses (DuckDB table)
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
from transforms.schemas.column_maps import ISSUED_LICENSES, LICENSES_CASTS

TABLE_NAME = "issued_licenses"
CSV_FILENAME = "Issued_Licenses_20260401.csv"


@asset(
    group_name="raw",
    description="NYC DCA issued business licenses with point geometry.",
)
def issued_licenses(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
    duckdb: DuckdbResource,
) -> None:
    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading issued licenses from {csv_path}")

    with duckdb.connection() as con:
        rel = (
            from_csv(con, csv_path.as_posix())
            .pipe(rename_columns, columns=ISSUED_LICENSES)
            .pipe(parse_point_from_lat_lon, lat_col="latitude", lon_col="longitude")
            .pipe(cast_columns, casts=LICENSES_CASTS)
        )
        row_count = write_table(con, rel, TABLE_NAME)

        active_count = con.execute(
            f"SELECT count(*) FROM {TABLE_NAME} WHERE license_status = 'Active'"
        ).fetchone()
        geocoded_count = con.execute(
            f"SELECT count(*) FROM {TABLE_NAME} WHERE geometry IS NOT NULL"
        ).fetchone()

    active = int(active_count[0]) if active_count else 0
    geocoded = int(geocoded_count[0]) if geocoded_count else 0

    context.log.info(
        f"Wrote {row_count} licenses to {TABLE_NAME} ({active} active, {geocoded} geocoded)"
    )
    context.add_output_metadata(
        {
            "num_licenses": MetadataValue.int(row_count),
            "num_active": MetadataValue.int(active),
            "num_geocoded": MetadataValue.int(geocoded),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
