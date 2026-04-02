"""Export derived_buildings + derived_businesses as JSON for vessel-studio seeding."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parent.parent / "data" / "buildings.duckdb"
OUTPUT_PATH = (
    Path(__file__).parent.parent.parent
    / "web-app-fullstack-vessel-studio"
    / "data"
    / "derived-buildings.json"
)

BOROUGH_FROM_BBL = {
    "1": "MANHATTAN",
    "2": "BRONX",
    "3": "BROOKLYN",
    "4": "QUEENS",
    "5": "STATEN ISLAND",
}

FIT_TIER_RANK = {
    "tier_1_high_fit": 1,
    "tier_2_strong_potential": 2,
    "tier_3_worth_exploring": 3,
}


def blank(val: str | None) -> str | None:
    """Return None for empty/whitespace-only strings."""
    if val is None or str(val).strip() == "" or str(val).strip() == "0":
        return None
    return str(val).strip()


def pick_primary(businesses: list[dict]) -> tuple[str | None, str | None]:
    """Pick the best subcategory and fit_category from a list of businesses.

    Returns (primarySubcategory, bestFitCategory).
    Picks highest tier first, then most common subcategory within that tier.
    """
    if not businesses:
        return None, None

    best_tier = min(
        (FIT_TIER_RANK.get(b["fit_category"], 99) for b in businesses),
    )
    best_fit = next(
        (k for k, v in FIT_TIER_RANK.items() if v == best_tier),
        None,
    )

    # Among best-tier businesses, pick most common subcategory
    tier_businesses = [
        b for b in businesses if FIT_TIER_RANK.get(b["fit_category"], 99) == best_tier
    ]
    subcategory_counts: dict[str, int] = {}
    for b in tier_businesses:
        sc = b.get("subcategory")
        if sc:
            subcategory_counts[sc] = subcategory_counts.get(sc, 0) + 1

    primary_sub = max(subcategory_counts, key=subcategory_counts.get) if subcategory_counts else None

    return primary_sub, best_fit


def build_name(
    building_name: str | None,
    businesses: list[dict],
    bin_val: str | None,
) -> str:
    """Build a display name with fallback chain."""
    name = blank(building_name)
    if name:
        return name

    # Try best business name
    for b in sorted(
        businesses,
        key=lambda x: FIT_TIER_RANK.get(x.get("fit_category", ""), 99),
    ):
        biz_name = blank(b.get("name"))
        if biz_name:
            return biz_name

    if bin_val:
        return f"BIN {bin_val}"

    return "Unknown Building"


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.install_extension("spatial")
    con.load_extension("spatial")

    print(f"Reading from {DB_PATH}")

    # Get all buildings with centroid coordinates
    buildings = con.execute("""
        SELECT
            bin,
            name,
            base_bbl,
            construction_year,
            height_roof,
            ground_elevation,
            shape_area,
            is_landmark,
            historic_districts,
            landmark_styles,
            landmark_architects,
            landmark_materials,
            landmark_original_uses,
            is_individual_landmark,
            landmark_entry_count,
            has_sbs_business,
            sbs_business_count,
            sbs_business_names,
            sbs_certification_types,
            sbs_ethnicities,
            sbs_naics_sectors,
            ST_X(centroid) AS lng,
            ST_Y(centroid) AS lat
        FROM derived_buildings
    """).fetchall()

    building_cols = [
        "bin", "name", "base_bbl", "construction_year",
        "height_roof", "ground_elevation", "shape_area",
        "is_landmark", "historic_districts", "landmark_styles",
        "landmark_architects", "landmark_materials", "landmark_original_uses",
        "is_individual_landmark", "landmark_entry_count",
        "has_sbs_business", "sbs_business_count", "sbs_business_names",
        "sbs_certification_types", "sbs_ethnicities", "sbs_naics_sectors",
        "lng", "lat",
    ]

    print(f"  {len(buildings)} buildings loaded")

    # Get all businesses with their containing building BIN via spatial join
    biz_rows = con.execute("""
        SELECT
            b.bin AS building_bin,
            biz.name,
            biz.dba_name,
            biz.subcategory,
            biz.fit_category,
            biz.website,
            biz.phone,
            biz.address_street,
            biz.address_city,
            biz.source_id
        FROM derived_buildings b
        INNER JOIN derived_businesses biz
            ON ST_Within(biz.geometry, b.geometry)
    """).fetchall()

    biz_cols = [
        "building_bin", "name", "dba_name", "subcategory", "fit_category",
        "website", "phone", "address_street", "address_city", "source_id",
    ]

    print(f"  {len(biz_rows)} business-building matches loaded")

    # Group businesses by building BIN
    biz_by_bin: dict[str, list[dict]] = {}
    for row in biz_rows:
        biz = dict(zip(biz_cols, row))
        building_bin = biz.pop("building_bin")
        if building_bin not in biz_by_bin:
            biz_by_bin[building_bin] = []
        biz_by_bin[building_bin].append(biz)

    # Build output
    results = []
    for row in buildings:
        bldg = dict(zip(building_cols, row))
        bin_val = bldg["bin"]
        businesses = biz_by_bin.get(bin_val, [])
        primary_sub, best_fit = pick_primary(businesses)
        bbl = bldg["base_bbl"]
        borough = BOROUGH_FROM_BBL.get(bbl[0] if bbl else "", None)

        venue = {
            "name": build_name(bldg["name"], businesses, bin_val),
            "bin": bin_val,
            "bbl": bbl,
            "borough": borough,
            "city": "New York",
            "state": "NY",
            "latitude": bldg["lat"],
            "longitude": bldg["lng"],
            "constructionYear": blank(bldg["construction_year"]),
            "heightRoof": bldg["height_roof"],
            "groundElevation": bldg["ground_elevation"],
            "shapeArea": bldg["shape_area"],
            "isLandmark": bool(bldg["is_landmark"]),
            "isIndividualLandmark": bool(bldg["is_individual_landmark"]),
            "historicDistrict": blank(bldg["historic_districts"]),
            "stylePrimary": blank(bldg["landmark_styles"]),
            "architectBuilder": blank(bldg["landmark_architects"]),
            "materialPrimary": blank(bldg["landmark_materials"]),
            "originalUse": blank(bldg["landmark_original_uses"]),
            "landmarkEntryCount": bldg["landmark_entry_count"] or 0,
            "hasSbsBusiness": bool(bldg["has_sbs_business"]),
            "sbsBusinessCount": bldg["sbs_business_count"] or 0,
            "sbsBusinessNames": blank(bldg["sbs_business_names"]),
            "sbsCertificationTypes": blank(bldg["sbs_certification_types"]),
            "primarySubcategory": primary_sub,
            "bestFitCategory": best_fit,
            "businessCount": len(businesses),
            "containedBusinesses": [
                {
                    "name": b.get("name"),
                    "dbaName": b.get("dba_name"),
                    "subcategory": b.get("subcategory"),
                    "fitCategory": b.get("fit_category"),
                    "website": b.get("website"),
                    "phone": b.get("phone"),
                    "addressStreet": b.get("address_street"),
                    "addressCity": b.get("address_city"),
                    "sourceId": b.get("source_id"),
                }
                for b in businesses
            ],
        }
        results.append(venue)

    con.close()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=None)

    print(f"Wrote {len(results)} venues to {OUTPUT_PATH}")
    print(f"  With businesses: {sum(1 for r in results if r['businessCount'] > 0)}")
    print(f"  Landmarks: {sum(1 for r in results if r['isLandmark'])}")


if __name__ == "__main__":
    main()
