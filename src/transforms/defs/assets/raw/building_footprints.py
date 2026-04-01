"""Asset that loads NYC building footprints from CSV into DuckDB."""

from __future__ import annotations

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "building_footprints"
CSV_FILENAME = "BUILDING_20260401.csv"


@asset(
    group_name="raw",
    description="NYC building footprints loaded from CSV into DuckDB with spatial geometry.",
)
def building_footprints(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Read the building footprints CSV and write to DuckDB with spatial geometry.

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing upload and database paths.
    """
    import duckdb

    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading building footprints from {csv_path}")

    db_path = pipeline_paths.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.install_extension("spatial")
    con.load_extension("spatial")

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            ST_GeomFromText(the_geom) AS geometry,
            "NAME",
            "BIN",
            "DOITT_ID",
            CAST("SHAPE_AREA" AS DOUBLE) AS shape_area,
            "BASE_BBL",
            "OBJECTID",
            "Construction Year" AS construction_year,
            "Feature Code" AS feature_code,
            "Geometry Source" AS geometry_source,
            "Ground Elevation" AS ground_elevation,
            "Height Roof" AS height_roof,
            "LAST_EDITED_DATE" AS last_edited_date,
            "LAST_STATUS_TYPE" AS last_status_type,
            "Map Pluto BBL" AS map_pluto_bbl,
            CAST("Length" AS DOUBLE) AS length
        FROM read_csv('{csv_path}', auto_detect=true, header=true)
        WHERE the_geom IS NOT NULL AND the_geom != ''
    """)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    con.close()

    context.log.info(f"Wrote {row_count} building footprints to {db_path}:{TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_buildings": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
