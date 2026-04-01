"""Asset that adds centroid points to landmark buildings."""

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "landmark_centroids"
SOURCE_TABLE = "landmarks"


@asset(
    group_name="derived",
    description="Landmark buildings with an additional centroid point column computed from geometry.",
    deps=["landmarks"],
)
def landmark_centroids(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Compute centroid for each landmark building and store as a new table.

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing database path.
    """
    import duckdb

    con = duckdb.connect(str(pipeline_paths.db_path))
    con.install_extension("spatial")
    con.load_extension("spatial")

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            *,
            ST_Centroid(geometry) AS centroid
        FROM {SOURCE_TABLE}
    """)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    sample = con.execute(f"""
        SELECT ST_AsText(centroid) FROM {TABLE_NAME} WHERE centroid IS NOT NULL LIMIT 1
    """).fetchone()
    con.close()

    sample_centroid = sample[0] if sample else "N/A"

    context.log.info(
        f"Wrote {row_count} landmarks with centroids to {pipeline_paths.db_path}:{TABLE_NAME}"
    )
    context.add_output_metadata(
        {
            "num_landmarks": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(pipeline_paths.db_path)),
            "sample_centroid": MetadataValue.text(sample_centroid),
        }
    )
