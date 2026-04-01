"""Asset that loads NYC DCA issued business licenses from CSV into DuckDB."""

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "issued_licenses"
CSV_FILENAME = "Issued_Licenses_20260401.csv"


@asset(
    group_name="raw",
    description="NYC DCA issued business licenses loaded from CSV into DuckDB with point geometry.",
)
def issued_licenses(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Read the issued licenses CSV and write to DuckDB with point geometry.

    Rows without coordinates get NULL geometry and are still included.

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing upload and database paths.
    """
    import duckdb

    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading issued licenses from {csv_path}")

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
                WHEN "Latitude" IS NOT NULL AND TRIM("Latitude") != ''
                    AND "Longitude" IS NOT NULL AND TRIM("Longitude") != ''
                THEN ST_Point(
                    CAST("Longitude" AS DOUBLE),
                    CAST("Latitude" AS DOUBLE)
                )
                ELSE NULL
            END AS geometry,
            "License Number" AS license_number,
            "Business Name" AS business_name,
            "DBA/Trade Name" AS dba_trade_name,
            "Business Unique ID" AS business_unique_id,
            "Business Category" AS business_category,
            "License Type" AS license_type,
            "License Status" AS license_status,
            "Initial Issuance Date" AS initial_issuance_date,
            "Expiration Date" AS expiration_date,
            "Details" AS details,
            "Contact Phone" AS contact_phone,
            "Address Type" AS address_type,
            "Building Number" AS building_number,
            "Street1" AS street1,
            "Street2" AS street2,
            "Street3" AS street3,
            "Unit Type" AS unit_type,
            "Apt/Suite" AS apt_suite,
            "City" AS city,
            "State" AS state,
            "ZIP Code" AS zip_code,
            "Borough" AS borough,
            "Community Board" AS community_board,
            "Council District" AS council_district,
            "BIN" AS bin,
            "BBL" AS bbl,
            "NTA" AS nta,
            "Census Block (2010)" AS census_block_2010,
            "Census Tract (2010)" AS census_tract_2010,
            CASE
                WHEN "Latitude" IS NOT NULL AND TRIM("Latitude") != ''
                THEN CAST("Latitude" AS DOUBLE)
                ELSE NULL
            END AS latitude,
            CASE
                WHEN "Longitude" IS NOT NULL AND TRIM("Longitude") != ''
                THEN CAST("Longitude" AS DOUBLE)
                ELSE NULL
            END AS longitude
        FROM read_csv('{csv_path}', all_varchar=true, header=true)
    """)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    active_count = con.execute(
        f"SELECT count(*) FROM {TABLE_NAME} WHERE license_status = 'Active'"
    ).fetchone()[0]
    geocoded_count = con.execute(
        f"SELECT count(*) FROM {TABLE_NAME} WHERE geometry IS NOT NULL"
    ).fetchone()[0]
    con.close()

    context.log.info(
        f"Wrote {row_count} licenses to {db_path}:{TABLE_NAME} "
        f"({active_count} active, {geocoded_count} geocoded)"
    )
    context.add_output_metadata(
        {
            "num_licenses": MetadataValue.int(row_count),
            "num_active": MetadataValue.int(active_count),
            "num_geocoded": MetadataValue.int(geocoded_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
