"""Asset: building footprints enriched with landmark and SBS business data.

Inputs : building_footprints, landmarks, sbs_certified_businesses,
         derived_businesses, 90_minute_radius_geojson.geojson
Output : derived_buildings (DuckDB table)
"""


from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.defs.resources.pipeline_paths import PipelinePaths
from transforms.logic.transforms.aggregate_landmarks_by_bin import (
    aggregate_landmarks_by_bin,
)
from transforms.logic.transforms.aggregate_sbs_by_bin import aggregate_sbs_by_bin
from transforms.logic.transforms.find_buildings_containing_businesses import (
    find_buildings_containing_businesses,
)
from transforms.logic.transforms.spatial_within_boundary import load_boundary

TABLE_NAME = "derived_buildings"
BOUNDARY_FILENAME = "90_minute_radius_geojson.geojson"


@asset(
    group_name="derived",
    description="Building footprints enriched with landmark status and SBS business data, filtered to exhibition-relevant venues.",
    deps=[
        "building_footprints",
        "landmarks",
        "sbs_certified_businesses",
        "derived_businesses",
    ],
)
def derived_buildings(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
    duckdb: DuckdbResource,
) -> None:
    boundary_path = pipeline_paths.uploads_path / BOUNDARY_FILENAME

    with duckdb.connection() as con:
        context.log.info("Aggregating landmarks by BIN")
        aggregate_landmarks_by_bin(con).create("landmarks_agg")

        context.log.info("Aggregating SBS businesses by BIN")
        aggregate_sbs_by_bin(con).create("sbs_agg")

        context.log.info(f"Loading boundary polygon from {boundary_path}")
        load_boundary(con, geojson_path=boundary_path.as_posix())

        context.log.info("Identifying buildings containing exhibition-relevant businesses")
        find_buildings_containing_businesses(con).create("matched_bins")
        matched_row = con.execute("SELECT count(*) FROM matched_bins").fetchone()
        matched = int(matched_row[0]) if matched_row else 0
        context.log.info(f"Found {matched} buildings containing relevant businesses")

        context.log.info("Joining building footprints with aggregated data")
        con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        con.execute(
            f"""
            CREATE TABLE {TABLE_NAME} AS
            SELECT
                b.*,
                ST_Centroid(b.geometry) AS centroid,
                COALESCE(l.is_landmark, FALSE) AS is_landmark,
                l.historic_districts,
                l.styles AS landmark_styles,
                l.architects AS landmark_architects,
                l.materials AS landmark_materials,
                l.original_uses AS landmark_original_uses,
                COALESCE(l.is_individual_landmark, FALSE) AS is_individual_landmark,
                COALESCE(l.landmark_entry_count, 0) AS landmark_entry_count,
                COALESCE(s.has_sbs_business, FALSE) AS has_sbs_business,
                COALESCE(s.sbs_business_count, 0) AS sbs_business_count,
                s.sbs_certification_types,
                s.sbs_business_names,
                s.sbs_ethnicities,
                s.sbs_naics_sectors
            FROM building_footprints b
            INNER JOIN matched_bins m ON b.bin = m.bin
            LEFT JOIN landmarks_agg l ON b.bin = l.bin
            LEFT JOIN sbs_agg s ON b.bin = s.bin
            """
        )

        row_row = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()
        landmark_row = con.execute(
            f"SELECT count(*) FROM {TABLE_NAME} WHERE is_landmark = TRUE"
        ).fetchone()
        sbs_row = con.execute(
            f"SELECT count(*) FROM {TABLE_NAME} WHERE has_sbs_business = TRUE"
        ).fetchone()
        col_count = len(con.execute(f"DESCRIBE {TABLE_NAME}").fetchall())

    row_count = int(row_row[0]) if row_row else 0
    landmark_count = int(landmark_row[0]) if landmark_row else 0
    sbs_count = int(sbs_row[0]) if sbs_row else 0

    context.log.info(
        f"Wrote {row_count} enriched buildings to {TABLE_NAME} "
        f"({landmark_count} landmarks, {sbs_count} with SBS businesses)"
    )
    context.add_output_metadata(
        {
            "num_buildings": MetadataValue.int(row_count),
            "num_columns": MetadataValue.int(col_count),
            "buildings_with_landmark": MetadataValue.int(landmark_count),
            "buildings_with_sbs": MetadataValue.int(sbs_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
        }
    )
