"""Asset that loads NY State active corporations from CSV into DuckDB."""

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "active_corporations"
CSV_FILENAME = "Active_Corporations___Beginning_1800.csv"


@asset(
    group_name="raw",
    description="NY State active corporations loaded from CSV into DuckDB.",
)
def active_corporations(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Read the active corporations CSV and write to DuckDB.

    This dataset has no coordinates. Address fields (location and
    DOS process address) are preserved for downstream geocoding or joins.

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing upload and database paths.
    """
    import duckdb

    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading active corporations from {csv_path}")

    db_path = pipeline_paths.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            "DOS ID" AS dos_id,
            "Current Entity Name" AS entity_name,
            "Initial DOS Filing Date" AS initial_filing_date,
            "County" AS county,
            "Jurisdiction" AS jurisdiction,
            "Entity Type" AS entity_type,
            "DOS Process Name" AS dos_process_name,
            "DOS Process Address 1" AS dos_process_address_1,
            "DOS Process Address 2" AS dos_process_address_2,
            "DOS Process City" AS dos_process_city,
            "DOS Process State" AS dos_process_state,
            "DOS Process Zip" AS dos_process_zip,
            "CEO Name" AS ceo_name,
            "CEO Address 1" AS ceo_address_1,
            "CEO Address 2" AS ceo_address_2,
            "CEO City" AS ceo_city,
            "CEO State" AS ceo_state,
            "CEO Zip" AS ceo_zip,
            "Registered Agent Name" AS registered_agent_name,
            "Registered Agent Address 1" AS registered_agent_address_1,
            "Registered Agent Address 2" AS registered_agent_address_2,
            "Registered Agent City" AS registered_agent_city,
            "Registered Agent State" AS registered_agent_state,
            "Registered Agent Zip" AS registered_agent_zip,
            "Location Name" AS location_name,
            "Location Address 1" AS location_address_1,
            "Location Address 2" AS location_address_2,
            "Location City" AS location_city,
            "Location State" AS location_state,
            "Location Zip" AS location_zip
        FROM read_csv('{csv_path}', all_varchar=true, header=true)
    """)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    con.close()

    context.log.info(f"Wrote {row_count} corporations to {db_path}:{TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_corporations": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
