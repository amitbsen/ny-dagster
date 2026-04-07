"""Asset: NY State retail food stores with point geometry parsed from WKT.

Inputs : Retail_Food_Stores.csv
Output : retail_food_stores (DuckDB table)
"""


from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.defs.resources.pipeline_paths import PipelinePaths
from transforms.lib.duckdb_rel import from_csv, write_table
from transforms.logic.transforms.cast_columns import cast_columns
from transforms.logic.transforms.parse_wkt_geometry import parse_wkt_geometry
from transforms.logic.transforms.rename_columns import rename_columns
from transforms.schemas.column_maps import RETAIL_FOOD_CASTS, RETAIL_FOOD_STORES

TABLE_NAME = "retail_food_stores"
CSV_FILENAME = "Retail_Food_Stores.csv"


@asset(
    group_name="raw",
    description="NY State retail food stores with point geometry parsed from WKT.",
)
def retail_food_stores(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
    duckdb: DuckdbResource,
) -> None:
    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading retail food stores from {csv_path}")

    with duckdb.connection() as con:
        rel = (
            from_csv(con, csv_path.as_posix())
            .pipe(rename_columns, columns=RETAIL_FOOD_STORES)
            .pipe(cast_columns, casts=RETAIL_FOOD_CASTS)
            .pipe(parse_wkt_geometry, source_col="georeference")
        )
        row_count = write_table(con, rel, TABLE_NAME)
        geocoded_row = con.execute(
            f"SELECT count(*) FROM {TABLE_NAME} WHERE geometry IS NOT NULL"
        ).fetchone()

    geocoded = int(geocoded_row[0]) if geocoded_row else 0
    context.log.info(
        f"Wrote {row_count} retail food stores to {TABLE_NAME} ({geocoded} geocoded)"
    )
    context.add_output_metadata(
        {
            "num_stores": MetadataValue.int(row_count),
            "num_geocoded": MetadataValue.int(geocoded),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
