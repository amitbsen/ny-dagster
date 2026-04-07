# CLAUDE.md

This file defines the code standards, architectural rules, and conventions for this repository. All contributors (human and LLM) must follow these rules when writing, reviewing, or modifying code.


## Project Architecture

This project is a Dagster data pipeline built with Python, managed by `uv` and the `dg` CLI. All source code lives under `src/transforms/`. The root directory contains only configuration files, documentation, and CI settings.

This repo uses Dagster's `load_from_defs_folder()` pattern. All assets, resources, jobs, schedules, and sensors are defined as modules inside `src/transforms/defs/`. The definitions entry point (`definitions.py`) auto-discovers them. Do not register definitions manually.

```
src/transforms/
  __init__.py
  definitions.py          # Entry point: @definitions + load_from_defs_folder()
  defs/
    assets/
      raw/                # One file per input source (CSV / API -> DuckDB)
      derived/            # One file per derived dataset
    resources/
      pipeline_paths.py   # Filesystem paths
      duckdb_io.py        # DuckdbResource (connection + spatial extension)
  lib/
    duckdb_rel.py         # .pipe() monkey-patch, from_csv, from_table, write_table
  logic/
    transforms/           # Pure (DuckDBPyRelation -> DuckDBPyRelation) functions
    expressions/          # Reusable SQL expression fragments and CASE builders
  schemas/
    column_maps.py        # Source -> target rename dicts + numeric cast dicts
  utils/                  # Non-Dagster helpers (snake_case, overpass client, ...)
tests/
  conftest.py             # In-memory DuckDB + spatial fixture
  test_logic/             # One test per logic transform
```

## Transform pattern

Every dataset is produced by exactly one thin asset file. The asset body only **declares I/O and composes `.pipe()` steps** — no inline SQL longer than one line, no renaming logic, no computed expressions. All real work lives in `logic/`:

- **`logic/transforms/*.py`** — pure functions with signature `(rel: DuckDBPyRelation, *, **kwargs) -> DuckDBPyRelation`. Side-effect free, no logging, no connection handling. Composable via `.pipe()`.
- **`logic/expressions/*.py`** — reusable SQL expression fragments (CASE builders, SELECT projections) as module constants.
- **`schemas/column_maps.py`** — each dataset's source->target rename dict and numeric cast dict lives here, one block per asset.

### The canonical asset template

```python
from dagster import AssetExecutionContext, MetadataValue, asset
from transforms.defs.resources.duckdb_io import DuckdbResource
from transforms.defs.resources.pipeline_paths import PipelinePaths
from transforms.lib.duckdb_rel import from_csv, write_table
from transforms.logic.transforms.rename_columns import rename_columns
from transforms.logic.transforms.cast_columns import cast_columns
from transforms.logic.transforms.parse_wkt_geometry import parse_wkt_geometry
from transforms.logic.transforms.filter_non_null_geometry import filter_non_null_geometry
from transforms.schemas.column_maps import LANDMARKS, LANDMARKS_CASTS

@asset(group_name="raw", description="...")
def landmarks(
    context: AssetExecutionContext,
    pipeline_paths: PipelinePaths,
    duckdb: DuckdbResource,
) -> None:
    csv_path = pipeline_paths.uploads_path / "Landmarks.csv"
    with duckdb.connection() as con:
        rel = (
            from_csv(con, csv_path.as_posix())
            .pipe(rename_columns, columns=LANDMARKS)
            .pipe(cast_columns, casts=LANDMARKS_CASTS)
            .pipe(parse_wkt_geometry, source_col="the_geom")
            .pipe(filter_non_null_geometry)
        )
        row_count = write_table(con, rel, "landmarks")
    context.add_output_metadata({"num_rows": MetadataValue.int(row_count)})
```

### Adding a new dataset

1. Add a source->target column map dict to `schemas/column_maps.py` (plus a casts dict if any columns need numeric conversion).
2. Create a single file in `defs/assets/raw/<dataset_name>.py` or `defs/assets/derived/<dataset_name>.py`.
3. Compose the work as a `.pipe()` chain using existing `logic/transforms/` functions.
4. If a transform step doesn't exist yet, add a new pure function to `logic/transforms/` and a unit test in `tests/test_logic/`. Do not write the logic inline in the asset.

### Rules

- Asset bodies must not contain column-rename SQL, CASE statements, or multi-line SQL string literals. Extract to `logic/`.
- Always acquire DuckDB connections via `DuckdbResource.connection()`. Never call `duckdb.connect()` from an asset.
- Every new `logic/transforms/*.py` function gets a unit test under `tests/test_logic/`.
- Numeric casts go through `cast_columns` so commas and blanks are tolerated uniformly.
- Geometry columns are always named `geometry`; centroids are always named `centroid`.


## Asset Design Rules

### One Asset Per Function

Each `@asset`-decorated function produces exactly one asset. If a function computes multiple outputs, use `@multi_asset` and declare each output explicitly.

### Assets Are Pure Data Transforms

An asset function receives its upstream dependencies as arguments and returns its output. Side effects (file I/O, API calls, database writes) happen through resources injected via the `resources` parameter, never through direct imports or global state.

```python
# Do this
@asset
def clean_parcels(raw_parcels: pl.DataFrame) -> pl.DataFrame:
    return raw_parcels.filter(pl.col("area_sqft") > 0)

# Not this
@asset
def clean_parcels() -> pl.DataFrame:
    df = pl.read_parquet("data/raw_parcels.parquet")  # Side effect
    return df.filter(pl.col("area_sqft") > 0)
```

### Group Assets by Domain

Assets that belong to the same data domain live in the same module under `defs/assets/`. Use Dagster's `group_name` parameter to organize them in the UI.

```python
@asset(group_name="reforestation")
def parcel_scores(...) -> pl.DataFrame: ...
```

### Partition and Schedule Definitions Stay Separate

Partition definitions and schedule definitions are not embedded in asset files. They live in `defs/schedules/` and reference assets by key.


## Resource Pattern

Resources encapsulate all external dependencies: file systems, APIs, database connections, configuration.

### Define Resources as Dagster ConfigurableResource Classes

```python
from dagster import ConfigurableResource

class PMTilesConfig(ConfigurableResource):
    output_dir: str
    min_zoom: int = 0
    max_zoom: int = 14
```

### Inject Resources, Never Import Them Directly

Asset and op functions receive resources through Dagster's dependency injection. Never instantiate a resource inside an asset.

```python
# Do this
@asset
def build_tiles(context, pmtiles_config: PMTilesConfig) -> None: ...

# Not this
@asset
def build_tiles(context) -> None:
    config = PMTilesConfig(output_dir="data/tiles")  # Direct instantiation
```

### Dev vs. Production Resources

Define separate resource configurations for development and production. The swap happens in `definitions.py` or via environment variables, not inside asset code.


## Import Direction

Code flows in one direction: `utils/` and `types/` -> `lib/` -> `defs/resources/` -> `defs/assets/` -> `defs/jobs/` -> `defs/schedules/` and `defs/sensors/`.

- `utils/` and `types/` have no internal imports
- `lib/` may import from `utils/` and `types/` only
- `defs/resources/` may import from `lib/`, `utils/`, and `types/` only
- `defs/assets/` may import from `defs/resources/`, `lib/`, `utils/`, and `types/`
- `defs/jobs/` may import from `defs/assets/` and `defs/resources/`
- `defs/schedules/` and `defs/sensors/` may import from `defs/jobs/` and `defs/assets/`
- No circular imports between asset modules


## Anti-Corruption Layer

Wrap third-party libraries in thin adapter modules under `lib/`. Assets and resources import from the adapter, never from the library directly. This limits the blast radius when a dependency ships breaking changes.

```python
# src/transforms/lib/geo.py
from shapely.geometry import shape, mapping  # noqa: F401
from pyproj import Transformer  # noqa: F401
```


## Naming Conventions

- Files and folders: `snake_case` (`clean_parcels.py`, `pmtiles_config.py`)
- Functions: `snake_case` (`build_tiles`, `fetch_parcel_data`)
- Asset functions: `snake_case` matching the asset key (`clean_parcels` produces the `clean_parcels` asset)
- Classes: `PascalCase` (`PMTilesConfig`, `ParcelScore`)
- Type aliases and TypedDicts: `PascalCase` (`ParcelRecord`, `TileConfig`)
- Constants: `UPPER_SNAKE_CASE` for true constants, `snake_case` for config objects
- Test files: `test_[name].py` alongside or mirroring source structure in `tests/`


## Function Rules

### Keep Functions Short

Asset functions should do one thing. If an asset function exceeds ~50 lines, extract helper functions into `utils/` or break the asset into upstream/downstream assets.

### Type Annotations Required

All function signatures must include type annotations for parameters and return values. Use `from __future__ import annotations` at the top of each file.

```python
from __future__ import annotations

import polars as pl
from dagster import asset

@asset
def clean_parcels(raw_parcels: pl.DataFrame) -> pl.DataFrame: ...
```

### Prefer Polars Over Pandas

Use Polars for all DataFrame operations unless a dependency requires Pandas. Polars is faster, uses less memory, and has a more explicit API.


## Testing

### Stack

- Unit and asset tests: pytest
- Dagster testing utilities: `dagster.build_asset_context`, `materialize_to_memory`
- Coverage: pytest-cov, 80% minimum threshold

### What to Test

- **Assets**: Test with `materialize_to_memory` or by calling the asset function directly with mock inputs. Verify output shape, column names, row counts, and edge cases.
- **Resources**: Test configuration validation and basic connectivity.
- **Utils and pure functions**: Unit test all non-trivial functions.
- **Jobs**: Test that job definitions resolve without errors. Test full job execution against fixtures for critical paths.

### Test File Location

```
tests/
  test_assets/
    test_clean_parcels.py
  test_resources/
    test_pmtiles_config.py
  test_utils/
    test_geo_helpers.py
```

### Test Fixtures

Use pytest fixtures for reusable test data. Define shared fixtures in `conftest.py`. Keep fixtures minimal and representative.

```python
@pytest.fixture
def sample_parcels() -> pl.DataFrame:
    return pl.DataFrame({
        "parcel_id": ["P001", "P002", "P003"],
        "area_sqft": [50000, 0, 217800],
        "borough": ["Manhattan", "Brooklyn", "Queens"],
    })
```


## Docstrings

All exported functions, classes, and modules must have docstrings. Use Google-style docstrings.

```python
def clean_parcels(raw_parcels: pl.DataFrame) -> pl.DataFrame:
    """Remove invalid parcels and normalize column names.

    Args:
        raw_parcels: Raw parcel data with original column names.

    Returns:
        Cleaned parcel DataFrame with standardized columns.
    """
```

### Asset Descriptions

Use the `description` parameter on `@asset` for Dagster UI documentation. Keep it to one sentence. Put detailed logic in the docstring.

```python
@asset(description="Cleaned parcel geometries with invalid entries removed.")
def clean_parcels(raw_parcels: pl.DataFrame) -> pl.DataFrame:
    """Remove invalid parcels and normalize column names.
    ...
    """
```


## Performance Defaults

- Use Polars lazy evaluation (`scan_parquet`, `.lazy()`, `.collect()`) for large datasets.
- Prefer `pl.scan_*` over `pl.read_*` to avoid loading full datasets into memory.
- Use Dagster partitions for time-series or geographically partitioned data rather than processing everything in a single run.
- Set `dagster/concurrency_key` metadata on assets that access rate-limited external services.


## Dagster-Specific Rules

- Never bypass Dagster's resource injection by importing clients or configs directly in asset code.
- Use `context.log` for all logging inside assets and ops. Do not use `print()` or the `logging` module directly.
- Use `MetadataValue` to attach row counts, file sizes, and other metrics to asset materializations.
- Use `AssetCheckSpec` and `@asset_check` for data quality validation rather than raising exceptions inside assets.
- Keep `definitions.py` minimal. It should contain only the `@definitions` decorator and `load_from_defs_folder()` call. Do not add logic there.
- Config values that change between environments belong in resources or Dagster config, not in module-level constants.


## Commands

```bash
uv sync               # Install all dependencies
dg dev                 # Start Dagster UI (localhost:3000)
uv run pytest tests/   # Run tests
uv run ruff check src/ # Lint
uv run ruff format src/# Format
uv run mypy src/       # Type check
```


## PR Checklist

Before merging any PR, all of the following must pass:

- `ruff check src/` (no lint errors)
- `ruff format --check src/` (formatting correct)
- `mypy src/` (no type errors)
- `pytest tests/` (all tests pass, coverage thresholds met)
- `dagster definitions validate` (all definitions load without errors)
- No circular imports between asset modules
- All new or modified exported symbols have docstrings
- Asset descriptions are set for all new assets
- Resources are injected, not instantiated inside assets
