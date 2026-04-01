"""Run the building footprints pipeline to populate DuckDB."""

from pathlib import Path

from dagster import materialize

from transforms.defs.assets.raw.building_footprints import building_footprints
from transforms.defs.assets.derived.building_centroids import building_centroids
from transforms.defs.resources.pipeline_paths import PipelinePaths

project_root = Path(__file__).parent

result = materialize(
    assets=[building_footprints, building_centroids],
    resources={
        "pipeline_paths": PipelinePaths(
            uploads_dir=str(project_root / "src" / "uploads"),
            duckdb_path=str(project_root / "data" / "buildings.duckdb"),
        ),
    },
)

if result.success:
    print("Pipeline completed successfully.")
else:
    for event in result.all_events:
        if event.is_failure:
            print(f"FAILED: {event}")
