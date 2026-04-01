"""Asset that loads SBS certified business list from CSV into DuckDB."""

from dagster import AssetExecutionContext, MetadataValue, asset

from transforms.defs.resources.pipeline_paths import PipelinePaths

TABLE_NAME = "sbs_certified_businesses"
CSV_FILENAME = "SBS_Certified_Business_List_20260401.csv"


@asset(
    group_name="raw",
    description="NYC SBS certified business list loaded from CSV into DuckDB with point geometry.",
)
def sbs_certified_businesses(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
) -> None:
    """Read the SBS certified business list CSV and write to DuckDB with point geometry.

    Args:
        context: Dagster execution context.
        pipeline_paths: Resource providing upload and database paths.
    """
    import duckdb

    csv_path = pipeline_paths.uploads_path / CSV_FILENAME
    context.log.info(f"Reading SBS certified businesses from {csv_path}")

    db_path = pipeline_paths.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.install_extension("spatial")
    con.load_extension("spatial")

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            ST_Point(
                CAST("Longitude" AS DOUBLE),
                CAST("Latitude" AS DOUBLE)
            ) AS geometry,
            "Account_Number" AS account_number,
            "Vendor_Formal_Name" AS vendor_formal_name,
            "Vendor_DBA" AS vendor_dba,
            "First_Name" AS first_name,
            "Last_Name" AS last_name,
            "telephone",
            "Business_Description" AS business_description,
            "Certification" AS certification,
            "Certification_Renewal_Date" AS certification_renewal_date,
            "Ethnicity" AS ethnicity,
            "Address_Line_1" AS address_line_1,
            "Address_Line_2" AS address_line_2,
            "City" AS city,
            "State" AS state,
            "Postcode" AS postcode,
            "Mailing_Address_Line_1" AS mailing_address_line_1,
            "Mailing_Address_Line_2" AS mailing_address_line_2,
            "Mailing_City" AS mailing_city,
            "Mailing_State" AS mailing_state,
            "Mailing_Zip" AS mailing_zip,
            "Website" AS website,
            "Date_Of_Establishment" AS date_of_establishment,
            "Aggregate_Bonding_Limit" AS aggregate_bonding_limit,
            "Signatory_To_Union_Contracts" AS signatory_to_union_contracts,
            "ID6_digit_NAICS_code" AS naics_code,
            "NAICS_Sector" AS naics_sector,
            "NAICS_Subsector" AS naics_subsector,
            "NAICS_Title" AS naics_title,
            "Types_of_Construction_Projects_Performed" AS construction_project_types,
            "NIGP_codes" AS nigp_codes,
            "Name_of_Client_Job_Exp_1" AS client_job_exp_1,
            "Largest_Value_of_Contract" AS largest_contract_value,
            "Percent_Self_Performed_Job_Exp_1" AS pct_self_performed_1,
            "Date_of_Work_Job_Exp_1" AS date_of_work_1,
            "Description_of_Work_Job_Exp_1" AS description_of_work_1,
            "Name_of_Client_Job_Exp_2" AS client_job_exp_2,
            "Value_of_Contract_Job_Exp_2" AS contract_value_2,
            "Percent_Self_Performed_Job_Exp_2" AS pct_self_performed_2,
            "Date_of_Work_Job_Exp_2" AS date_of_work_2,
            "Description_of_Work_Job_Exp_2" AS description_of_work_2,
            "Name_of_Client_Job_Exp_3" AS client_job_exp_3,
            "Value_of_Contract_Job_Exp_3" AS contract_value_3,
            "Percent_Self_Performed_Job_Exp_3" AS pct_self_performed_3,
            "Date_of_Work_Job_Exp_3" AS date_of_work_3,
            "Description_of_Work_Job_Exp_3" AS description_of_work_3,
            "Capacity_Building_Programs" AS capacity_building_programs,
            "Enrolled_in_PASSPort" AS enrolled_in_passport,
            "Borough" AS borough,
            CAST("Latitude" AS DOUBLE) AS latitude,
            CAST("Longitude" AS DOUBLE) AS longitude,
            "Community Board" AS community_board,
            "Council District" AS council_district,
            "BIN" AS bin,
            "BBL" AS bbl,
            "Census Tract (2020)" AS census_tract_2020,
            "Neighborhood Tabulation Area (NTA) (2020)" AS nta_2020
        FROM read_csv('{csv_path}', all_varchar=true, header=true)
        WHERE "Latitude" IS NOT NULL
            AND "Latitude" != ''
            AND "Longitude" IS NOT NULL
            AND "Longitude" != ''
    """)

    row_count = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    con.close()

    context.log.info(f"Wrote {row_count} businesses to {db_path}:{TABLE_NAME}")
    context.add_output_metadata(
        {
            "num_businesses": MetadataValue.int(row_count),
            "table": MetadataValue.text(TABLE_NAME),
            "duckdb_path": MetadataValue.path(str(db_path)),
            "source_csv": MetadataValue.path(str(csv_path)),
        }
    )
