"""Run all pipeline assets to populate DuckDB."""

from __future__ import annotations

from pathlib import Path

from dagster import materialize

from transforms.defs.assets.derived.building_centroids import building_centroids
from transforms.defs.assets.derived.derived_buildings import derived_buildings
from transforms.defs.assets.derived.derived_businesses import derived_businesses
from transforms.defs.assets.derived.landmark_centroids import landmark_centroids
from transforms.defs.assets.raw.active_corporations import active_corporations
from transforms.defs.assets.raw.building_footprints import building_footprints
from transforms.defs.assets.raw.issued_licenses import issued_licenses
from transforms.defs.assets.raw.landmarks import landmarks
from transforms.defs.assets.raw.osm_businesses import osm_businesses
from transforms.defs.assets.raw.retail_food_stores import retail_food_stores
from transforms.defs.assets.raw.sbs_certified_businesses import sbs_certified_businesses
from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.defs.resources.pipeline_paths import PipelinePaths

project_root = Path(__file__).parent
db_path = project_root / "data" / "buildings.duckdb"

result = materialize(
    assets=[
        building_footprints,
        building_centroids,
        landmarks,
        landmark_centroids,
        sbs_certified_businesses,
        issued_licenses,
        retail_food_stores,
        active_corporations,
        osm_businesses,
        derived_businesses,
        derived_buildings,
    ],
    resources={
        "pipeline_paths": PipelinePaths(
            uploads_dir=str(project_root / "src" / "uploads"),
            duckdb_path=str(db_path),
        ),
        "duckdb": DuckdbResource(duckdb_path=str(db_path)),
    },
)

if result.success:
    print("Pipeline completed successfully.")
else:
    for event in result.all_events:
        if event.is_failure:
            print(f"FAILED: {event}")
