"""Asset that enriches building footprints with landmark and SBS business data."""

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "derived_buildings"


@asset(
    group_name="derived",
    description="Building footprints enriched with landmark status and SBS certified business data.",
    deps=["building_footprints", "landmarks", "sbs_certified_businesses"],
)
def derived_buildings(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Join building footprints with landmarks and SBS businesses by BIN.

    Produces one row per building with additional columns from landmarks
    (historic district, style, architect, materials) and SBS businesses
    (certification count, types, business names).

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing database path.
    """
    import duckdb

    con = duckdb.connect(str(pipeline_paths.db_path))
    con.install_extension("spatial")
    con.load_extension("spatial")

    # Pre-aggregate landmarks to one row per BIN
    context.log.info("Aggregating landmarks by BIN")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE landmarks_agg AS
        SELECT
            bin,
            TRUE AS is_landmark,
            STRING_AGG(DISTINCT historic_district, '; ')
                FILTER (WHERE historic_district IS NOT NULL
                    AND TRIM(historic_district) != ''
                    AND historic_district != '0')
                AS historic_districts,
            STRING_AGG(DISTINCT style_primary, '; ')
                FILTER (WHERE style_primary IS NOT NULL
                    AND TRIM(style_primary) != ''
                    AND style_primary != '0'
                    AND style_primary != 'Not determined')
                AS styles,
            STRING_AGG(DISTINCT architect_builder, '; ')
                FILTER (WHERE architect_builder IS NOT NULL
                    AND TRIM(architect_builder) != ''
                    AND architect_builder != '0'
                    AND architect_builder != 'Not determined')
                AS architects,
            STRING_AGG(DISTINCT material_primary, '; ')
                FILTER (WHERE material_primary IS NOT NULL
                    AND TRIM(material_primary) != ''
                    AND material_primary != '0'
                    AND material_primary != 'Not determined')
                AS materials,
            STRING_AGG(DISTINCT use_original, '; ')
                FILTER (WHERE use_original IS NOT NULL
                    AND TRIM(use_original) != ''
                    AND use_original != '0'
                    AND use_original != 'Not determined')
                AS original_uses,
            BOOL_OR(landmark_original != '0' AND landmark_original != '')
                AS is_individual_landmark,
            COUNT(*) AS landmark_entry_count
        FROM landmarks
        WHERE bin IS NOT NULL AND TRIM(bin) != ''
        GROUP BY bin
    """)

    # Pre-aggregate SBS businesses to one row per BIN
    context.log.info("Aggregating SBS businesses by BIN")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE sbs_agg AS
        SELECT
            bin,
            TRUE AS has_sbs_business,
            COUNT(*) AS sbs_business_count,
            STRING_AGG(DISTINCT certification, '; ')
                FILTER (WHERE certification IS NOT NULL
                    AND TRIM(certification) != '')
                AS sbs_certification_types,
            STRING_AGG(DISTINCT vendor_formal_name, '; ')
                FILTER (WHERE vendor_formal_name IS NOT NULL
                    AND TRIM(vendor_formal_name) != '')
                AS sbs_business_names,
            STRING_AGG(DISTINCT ethnicity, '; ')
                FILTER (WHERE ethnicity IS NOT NULL
                    AND TRIM(ethnicity) != '')
                AS sbs_ethnicities,
            STRING_AGG(DISTINCT naics_sector, '; ')
                FILTER (WHERE naics_sector IS NOT NULL
                    AND TRIM(naics_sector) != '')
                AS sbs_naics_sectors
        FROM sbs_certified_businesses
        WHERE bin IS NOT NULL AND TRIM(bin) != ''
        GROUP BY bin
    """)

    # Join everything
    context.log.info("Joining building footprints with aggregated data")
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"""
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
        LEFT JOIN landmarks_agg l ON b.bin = l.bin
        LEFT JOIN sbs_agg s ON b.bin = s.bin
    """)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    landmark_count = con.execute(
        f"SELECT count(*) FROM {TABLE_NAME} WHERE is_landmark = TRUE"
    ).fetchone()[0]
    sbs_count = con.execute(
        f"SELECT count(*) FROM {TABLE_NAME} WHERE has_sbs_business = TRUE"
    ).fetchone()[0]
    col_count = len(con.execute(f"DESCRIBE {TABLE_NAME}").fetchall())
    con.close()

    context.log.info(
        f"Wrote {row_count} enriched buildings to {pipeline_paths.db_path}:{TABLE_NAME} "
        f"({landmark_count} landmarks, {sbs_count} with SBS businesses)"
    )
    context.add_output_metadata(
        {
            "num_buildings": MetadataValue.int(row_count),
            "num_columns": MetadataValue.int(col_count),
            "buildings_with_landmark": MetadataValue.int(landmark_count),
            "buildings_with_sbs": MetadataValue.int(sbs_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(pipeline_paths.db_path)),
        }
    )
