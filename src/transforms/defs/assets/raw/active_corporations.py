"""Asset: NY State active corporations (no geometry).

Inputs : Active_Corporations___Beginning_1800.csv
Output : active_corporations (DuckDB table)
"""


from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.defs.resources.pipeline_paths import PipelinePaths
from transforms.lib.duckdb_rel import from_csv, write_table
from transforms.logic.transforms.rename_columns import rename_columns
from transforms.schemas.column_maps import ACTIVE_CORPORATIONS

TABLE_NAME = "active_corporations"
CSV_FILENAME = "Active_Corporations___Beginning_1800.csv"


@asset(
    group_name="raw",
    description="NY State active corporations list (no geometry).",
)
def active_corporations(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
    duckdb: DuckdbResource,
) -> None:
    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading active corporations from {csv_path}")

    with duckdb.connection() as con:
        rel = from_csv(con, csv_path.as_posix()).pipe(
            rename_columns, columns=ACTIVE_CORPORATIONS
        )
        row_count = write_table(con, rel, TABLE_NAME)

    context.log.info(f"Wrote {row_count} corporations to {TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_corporations": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
