"""SELECT fragment projecting SBS certified businesses onto the harmonized schema."""

from __future__ import annotations

SELECT: str = """
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
"""
