"""SELECT fragment projecting OSM businesses onto the harmonized schema."""

from __future__ import annotations

SELECT: str = """
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
"""
