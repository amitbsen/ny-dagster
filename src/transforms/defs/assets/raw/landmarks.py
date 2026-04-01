"""Asset that loads NYC landmark and historic district buildings from CSV into DuckDB."""

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "landmarks"
CSV_FILENAME = "Individual_Landmark_and_Historic_District_Building_Database_20260401.csv"


@asset(
    group_name="raw",
    description="NYC individual landmark and historic district buildings loaded from CSV into DuckDB.",
)
def landmarks(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Read the landmarks CSV and write to DuckDB with spatial geometry.

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing upload and database paths.
    """
    import duckdb

    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading landmarks from {csv_path}")

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
            "BIN" AS bin,
            "BBL" AS bbl,
            "Borough" AS borough,
            "Block" AS block,
            "Lot" AS lot,
            "Des_Addres" AS designated_address,
            "Circa" AS circa,
            "Date_Low" AS date_low,
            "Date_High" AS date_high,
            "Date_Combo" AS date_combo,
            "Alt_Date_1" AS alt_date_1,
            "Alt_Date_2" AS alt_date_2,
            "Arch_Build" AS architect_builder,
            "Own_Devel" AS owner_developer,
            "Alt_Arch_1" AS alt_architect_1,
            "Alt_Arch_2" AS alt_architect_2,
            "Altered" AS altered,
            "Style_Prim" AS style_primary,
            "Style_Sec" AS style_secondary,
            "Style_Oth" AS style_other,
            "Mat_Prim" AS material_primary,
            "Mat_Sec" AS material_secondary,
            "Mat_Third" AS material_third,
            "Mat_Four" AS material_fourth,
            "Mat_Other" AS material_other,
            "Use_Orig" AS use_original,
            "Use_Other" AS use_other,
            "Build_Type" AS building_type,
            "Build_Oth" AS building_type_other,
            "Build_Nme" AS building_name,
            "Notes" AS notes,
            "Hist_Dist" AS historic_district,
            "LM_Orig" AS landmark_original,
            "LM_New" AS landmark_new,
            CAST(REPLACE("shape_leng", ',', '') AS DOUBLE) AS shape_length,
            CAST(REPLACE("shape_area", ',', '') AS DOUBLE) AS shape_area
        FROM read_csv('{csv_path}', all_varchar=true, header=true)
        WHERE the_geom IS NOT NULL AND the_geom != ''
    """)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    con.close()

    context.log.info(f"Wrote {row_count} landmarks to {db_path}:{TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_landmarks": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
