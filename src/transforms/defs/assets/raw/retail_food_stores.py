"""Asset that loads NY State retail food stores from CSV into DuckDB."""

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "retail_food_stores"
CSV_FILENAME = "Retail_Food_Stores.csv"


@asset(
    group_name="raw",
    description="NY State retail food stores loaded from CSV into DuckDB with point geometry.",
)
def retail_food_stores(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Read the retail food stores CSV and write to DuckDB with point geometry.

    The Georeference column contains WKT POINT strings. Rows without
    a georeference get NULL geometry and are still included.

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing upload and database paths.
    """
    import duckdb

    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading retail food stores from {csv_path}")

    db_path = pipeline_paths.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.install_extension("spatial")
    con.load_extension("spatial")

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            CASE
                WHEN "Georeference" IS NOT NULL AND TRIM("Georeference") != ''
                THEN ST_GeomFromText("Georeference")
                ELSE NULL
            END AS geometry,
            "County" AS county,
            "License Number" AS license_number,
            "Operation Type" AS operation_type,
            "Establishment Type" AS establishment_type,
            "Entity Name" AS entity_name,
            "DBA Name" AS dba_name,
            "Street Number" AS street_number,
            "Street Name" AS street_name,
            "Address Line 2" AS address_line_2,
            "Address Line 3" AS address_line_3,
            "City" AS city,
            "State" AS state,
            "Zip Code" AS zip_code,
            CASE
                WHEN "Square Footage" IS NOT NULL AND TRIM("Square Footage") != ''
                THEN CAST(REPLACE("Square Footage", ',', '') AS INTEGER)
                ELSE NULL
            END AS square_footage
        FROM read_csv('{csv_path}', all_varchar=true, header=true)
    """)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    geocoded_count = con.execute(
        f"SELECT count(*) FROM {TABLE_NAME} WHERE geometry IS NOT NULL"
    ).fetchone()[0]
    con.close()

    context.log.info(
        f"Wrote {row_count} retail food stores to {db_path}:{TABLE_NAME} "
        f"({geocoded_count} geocoded)"
    )
    context.add_output_metadata(
        {
            "num_stores": MetadataValue.int(row_count),
            "num_geocoded": MetadataValue.int(geocoded_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
