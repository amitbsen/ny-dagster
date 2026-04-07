"""SELECT fragment projecting retail food stores onto the harmonized schema."""

from __future__ import annotations

SELECT: str = """
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
"""
