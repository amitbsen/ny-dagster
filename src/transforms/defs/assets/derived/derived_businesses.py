"""Asset: harmonized businesses from OSM, DCA, retail food, and SBS sources.

Inputs : osm_businesses, issued_licenses, retail_food_stores,
         sbs_certified_businesses, 90_minute_radius_geojson.geojson
Output : derived_businesses (DuckDB table)
"""


from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.defs.resources.pipeline_paths import PipelinePaths
from transforms.lib.duckdb_rel import write_table
from transforms.logic.expressions.fit_category_case import ALL_FIT_SUBCATEGORIES
from transforms.logic.transforms.assign_fit_category import assign_fit_category
from transforms.logic.transforms.filter_in_values import filter_in_values
from transforms.logic.transforms.spatial_within_boundary import load_boundary
from transforms.logic.transforms.union_business_sources import union_business_sources

TABLE_NAME = "derived_businesses"
BOUNDARY_FILENAME = "90_minute_radius_geojson.geojson"


@asset(
    group_name="derived",
    description="Harmonized businesses from 4 sources, filtered to a boundary and tagged with a fit tier.",
    deps=[
        "osm_businesses",
        "issued_licenses",
        "retail_food_stores",
        "sbs_certified_businesses",
    ],
)
def derived_businesses(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
    duckdb: DuckdbResource,
) -> None:
    boundary_path = pipeline_paths.uploads_path / BOUNDARY_FILENAME
    context.log.info(f"Loading boundary polygon from {boundary_path}")

    with duckdb.connection() as con:
        load_boundary(con, geojson_path=boundary_path.as_posix())
        rel = (
            union_business_sources(con)
            .pipe(filter_in_values, column="subcategory", values=ALL_FIT_SUBCATEGORIES)
            .pipe(assign_fit_category)
        )
        row_count = write_table(con, rel, TABLE_NAME)

        geocoded_row = con.execute(
            f"SELECT count(*) FROM {TABLE_NAME} WHERE geometry IS NOT NULL"
        ).fetchone()
        source_rows = con.execute(
            f"SELECT source, count(*) FROM {TABLE_NAME} GROUP BY source ORDER BY source"
        ).fetchall()
        fit_rows = con.execute(
            f"SELECT fit_category, count(*) FROM {TABLE_NAME} "
            "WHERE fit_category IS NOT NULL GROUP BY fit_category ORDER BY fit_category"
        ).fetchall()

    geocoded = int(geocoded_row[0]) if geocoded_row else 0
    source_meta = {f"source_{r[0]}": MetadataValue.int(r[1]) for r in source_rows}
    fit_meta = {f"fit_{r[0]}": MetadataValue.int(r[1]) for r in fit_rows}
    total_fit = sum(r[1] for r in fit_rows)

    context.log.info(
        f"Wrote {row_count} harmonized businesses to {TABLE_NAME} "
        f"({geocoded} geocoded, {total_fit} tagged)"
    )
    context.add_output_metadata(
        {
            "num_businesses": MetadataValue.int(row_count),
            "num_geocoded": MetadataValue.int(geocoded),
            "num_with_fit_category": MetadataValue.int(total_fit),
            "num_sources": MetadataValue.int(len(source_rows)),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(duckdb.db_path)),
            **source_meta,
            **fit_meta,
        }
    )
