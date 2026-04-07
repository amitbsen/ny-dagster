"""SELECT fragment projecting DCA issued licenses onto the harmonized schema."""

from __future__ import annotations

SELECT: str = """
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
"""
