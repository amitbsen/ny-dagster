"""Asset: building footprints with an added centroid point column.

Inputs : building_footprints (DuckDB table)
Output : building_footprints_centroids (DuckDB table)
"""


from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.lib.duckdb_rel import from_table, write_table
from transforms.logic.transforms.compute_centroid import compute_centroid

TABLE_NAME = "building_footprints_centroids"
SOURCE_TABLE = "building_footprints"


@asset(
    group_name="derived",
    description="Building footprints with an added centroid point column.",
    deps=["building_footprints"],
)
def building_centroids(
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
    context.log.info(f"Wrote {row_count} buildings with centroids to {TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_buildings": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            "sample_centroid": MetadataValue.text(sample),
        }
    )
