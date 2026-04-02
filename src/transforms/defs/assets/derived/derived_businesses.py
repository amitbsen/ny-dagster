"""Asset that harmonizes all geocoded business sources into a single unified table."""

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "derived_businesses"


@asset(
    group_name="derived",
    description="Harmonized business data from OSM, DCA licenses, retail food stores, and SBS certified businesses.",
    deps=["osm_businesses", "issued_licenses", "retail_food_stores", "sbs_certified_businesses"],
)
def derived_businesses(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Union all geocoded business sources into a single table with a common schema.

    Harmonizes name, category, address, and contact fields across four
    sources. Each row retains its source identifier and source-specific
    category mapping.

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing database path.
    """
    import duckdb

    con = duckdb.connect(str(pipeline_paths.db_path))
    con.install_extension("spatial")
    con.load_extension("spatial")

    context.log.info("Creating harmonized business table from 4 sources")

    # Load boundary polygon for spatial filtering
    boundary_path = pipeline_paths.uploads_path / "90_minute_radius_geojson.geojson"
    context.log.info(f"Loading boundary polygon from {boundary_path}")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE boundary AS
        SELECT geom
        FROM ST_Read('{boundary_path.as_posix()}')
    """)

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            b.*,
            CASE
                WHEN b.subcategory IN ('cafe', 'gallery', 'books', 'florist', 'garden')
                    THEN 'tier_1_high_fit'
                WHEN b.subcategory IN ('bakery', 'bar', 'hotel', 'gift', 'community_centre')
                    THEN 'tier_2_strong_potential'
                WHEN b.subcategory IN ('library', 'museum', 'coworking', 'artwork')
                    THEN 'tier_3_worth_exploring'
                ELSE NULL
            END AS fit_category
        FROM (

        -- OSM businesses
        SELECT
            o.geometry,
            'osm' AS source,
            o.osm_type || '/' || CAST(o.osm_id AS VARCHAR) AS source_id,
            o.name,
            NULL AS dba_name,
            CASE
                WHEN o.amenity IS NOT NULL THEN 'amenity'
                WHEN o.shop IS NOT NULL THEN 'retail'
                WHEN o.tourism IS NOT NULL THEN 'tourism'
                WHEN o.craft IS NOT NULL THEN 'craft'
                WHEN o.office IS NOT NULL THEN 'office'
                WHEN o.leisure IS NOT NULL THEN 'leisure'
            END AS category,
            COALESCE(o.amenity, o.shop, o.tourism, o.craft, o.office, o.leisure) AS subcategory,
            CONCAT_WS(' ', o.addr_housenumber, o.addr_street) AS address_street,
            o.addr_city AS address_city,
            CAST(NULL AS VARCHAR) AS address_state,
            o.addr_postcode AS address_zip,
            CAST(NULL AS VARCHAR) AS borough,
            o.phone,
            o.website,
            o.latitude,
            o.longitude,
            CAST(NULL AS VARCHAR) AS bin,
            CAST(NULL AS VARCHAR) AS bbl
        FROM osm_businesses o
        CROSS JOIN boundary bnd
        WHERE ST_Within(o.geometry, bnd.geom)

        UNION ALL

        -- DCA issued licenses
        SELECT
            il.geometry,
            'dca_license' AS source,
            il.license_number AS source_id,
            il.business_name AS name,
            il.dba_trade_name AS dba_name,
            il.business_category AS category,
            il.license_type AS subcategory,
            CONCAT_WS(' ', il.building_number, il.street1) AS address_street,
            il.city AS address_city,
            il.state AS address_state,
            il.zip_code AS address_zip,
            il.borough,
            il.contact_phone AS phone,
            CAST(NULL AS VARCHAR) AS website,
            il.latitude,
            il.longitude,
            il.bin,
            il.bbl
        FROM issued_licenses il
        CROSS JOIN boundary bnd
        WHERE ST_Within(il.geometry, bnd.geom)

        UNION ALL

        -- Retail food stores
        SELECT
            rfs.geometry,
            'retail_food' AS source,
            rfs.license_number AS source_id,
            rfs.entity_name AS name,
            rfs.dba_name,
            'food_retail' AS category,
            rfs.establishment_type AS subcategory,
            CONCAT_WS(' ', rfs.street_number, rfs.street_name) AS address_street,
            rfs.city AS address_city,
            rfs.state AS address_state,
            rfs.zip_code AS address_zip,
            CAST(NULL AS VARCHAR) AS borough,
            CAST(NULL AS VARCHAR) AS phone,
            CAST(NULL AS VARCHAR) AS website,
            ST_Y(rfs.geometry) AS latitude,
            ST_X(rfs.geometry) AS longitude,
            CAST(NULL AS VARCHAR) AS bin,
            CAST(NULL AS VARCHAR) AS bbl
        FROM retail_food_stores rfs
        CROSS JOIN boundary bnd
        WHERE ST_Within(rfs.geometry, bnd.geom)

        UNION ALL

        -- SBS certified businesses
        SELECT
            sbs.geometry,
            'sbs_certified' AS source,
            sbs.account_number AS source_id,
            sbs.vendor_formal_name AS name,
            sbs.vendor_dba AS dba_name,
            sbs.naics_sector AS category,
            COALESCE(sbs.naics_title, sbs.certification) AS subcategory,
            sbs.address_line_1 AS address_street,
            sbs.city AS address_city,
            sbs.state AS address_state,
            sbs.postcode AS address_zip,
            sbs.borough,
            sbs.telephone AS phone,
            sbs.website,
            sbs.latitude,
            sbs.longitude,
            sbs.bin,
            sbs.bbl
        FROM sbs_certified_businesses sbs
        CROSS JOIN boundary bnd
        WHERE ST_Within(sbs.geometry, bnd.geom)

        ) b
    """)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    geocoded_count = con.execute(
        f"SELECT count(*) FROM {TABLE_NAME} WHERE geometry IS NOT NULL"
    ).fetchone()[0]
    source_counts = con.execute(
        f"SELECT source, count(*) FROM {TABLE_NAME} GROUP BY source ORDER BY source"
    ).fetchall()
    fit_counts = con.execute(
        f"SELECT fit_category, count(*) FROM {TABLE_NAME} "
        f"WHERE fit_category IS NOT NULL GROUP BY fit_category ORDER BY fit_category"
    ).fetchall()

    con.close()

    source_meta = {
        f"source_{row[0]}": MetadataValue.int(row[1]) for row in source_counts
    }
    fit_meta = {
        f"fit_{row[0]}": MetadataValue.int(row[1]) for row in fit_counts
    }
    total_fit = sum(row[1] for row in fit_counts)

    context.log.info(
        f"Wrote {row_count} harmonized businesses to {pipeline_paths.db_path}:{TABLE_NAME} "
        f"({geocoded_count} geocoded, {total_fit} with exhibition fit category)"
    )
    context.add_output_metadata(
        {
            "num_businesses": MetadataValue.int(row_count),
            "num_geocoded": MetadataValue.int(geocoded_count),
            "num_with_fit_category": MetadataValue.int(total_fit),
            "num_sources": MetadataValue.int(len(source_counts)),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(pipeline_paths.db_path)),
            **source_meta,
            **fit_meta,
        }
    )
