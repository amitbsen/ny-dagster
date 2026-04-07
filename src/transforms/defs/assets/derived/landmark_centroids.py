"""Asset: landmark buildings with an added centroid point column.

Inputs : landmarks (DuckDB table)
Output : landmark_centroids (DuckDB table)
"""


from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.lib.duckdb_rel import from_table, write_table
from transforms.logic.transforms.compute_centroid import compute_centroid

TABLE_NAME = "landmark_centroids"
SOURCE_TABLE = "landmarks"


@asset(
    group_name="derived",
    description="Landmark buildings with an added centroid point column.",
    deps=["landmarks"],
)
def landmark_centroids(
    context: AssetExecutionContext,
    duckdb: DuckdbResource,
) -> None:
    with duckdb.connection() as con:
        rel = from_table(con, SOURCE_TABLE).pipe(compute_centroid)
        row_count = write_table(con, rel, TABLE_NAME)
        sample_row = con.execute(
            f"SELECT ST_AsText(centroid) FROM {TABLE_NAME} "
            "WHERE centroid IS NOT NULL LIMIT 1"
        ).fetchone()

    sample = sample_row[0] if sample_row else "N/A"
    context.log.info(f"Wrote {row_count} landmarks with centroids to {TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_landmarks": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            "sample_centroid": MetadataValue.text(sample),
        }
    )
