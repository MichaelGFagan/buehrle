# Loader CLI conventions

Loaders should default to a small/recent scope (e.g. current season) and require an explicit flag for a full backfill. This protects against accidentally hammering external APIs or producing large loads.

Standard flags (see `loaders/mlb_statsapi/schedules.py` for the reference implementation):

- **No args** → narrow default scope (current season).
- **`--season` / `--start-season` / `--end-season`** → scope to a specific season or season range.
- **`--full-history`** → backfill from the earliest available data through the present. Mutually exclusive with `--season`/`--date` args.
- **`--full-refresh`** → orthogonal to scope: drops the pipeline's destination schema before loading. Combine with `--full-history` for a clean full backfill.

The `--full-history` and `--full-refresh` flags are independent because they answer different questions (*what range to load* vs. *whether to drop existing data first*). A user backfilling for the first time typically wants both; a daily incremental run wants neither.

Use `loaders/cli.py` to wire up the standard scope args. For season-only loaders:

```python
from loaders.cli import add_season_args, resolve_seasons, validate_season_args

parser = argparse.ArgumentParser()
add_season_args(parser, EARLIEST_SEASON)
parser.add_argument('--full-refresh', action='store_true')
parser.add_argument('--update', action='store_true')
args = parser.parse_args()
validate_season_args(parser, args)
start_season, end_season = resolve_seasons(args, EARLIEST_SEASON)
```

For loaders that also accept date args, add `add_date_args` and use `validate_scope_args` (which subsumes `validate_season_args`). The resolver depends on whether the backend can consume seasons natively or needs dates:

- **Backend accepts both seasons and dates** (e.g. MLB Stats API — see `loaders/mlb_statsapi/schedules.py`): use `resolve_scope`, which returns `{'seasons': list[int] | None, 'dates': (start, end) | None}` with exactly one key populated.
- **Backend is date-only** (e.g. Statcast pitches — see `loaders/statcast/statcast_pitches.py`): use `resolve_dates(args, EARLIEST_SEASON, season_bounds)`, which always returns `(start_date, end_date)`. `season_bounds(year) -> (start_date, end_date)` is required because seasonal date windows vary per source.

```python
from loaders.cli import add_date_args, add_season_args, resolve_scope, validate_scope_args

parser = argparse.ArgumentParser()
add_season_args(parser, EARLIEST_SEASON)
add_date_args(parser)
parser.add_argument('--full-refresh', action='store_true')
args = parser.parse_args()
validate_scope_args(parser, args)
scope = resolve_scope(args, EARLIEST_SEASON)  # or resolve_dates(...) for date-only backends
```

**Loaders intentionally exempt from this convention** (single-shot scrapes with no per-season slicing — they take `--full-refresh` only):

- `loaders/baseball_reference/baseball_reference_war.py`
- `loaders/chadwick/chadwick_register.py`
- `loaders/lahman/lahman.py`

# dlt patterns

## Full refresh

Do not pass `refresh='drop_sources'` inline with a source — dlt stages the drop as a pending load package, consuming the `run` call without extracting any data.

`pipeline.drop()` alone is also insufficient: it clears local pipeline state but leaves the destination tables intact. On the next run, dlt tries to merge into those existing tables without knowing their schema, which causes `NOT NULL constraint failed: _dlt_load_id`.

Each pipeline uses a `dataset_name` that matches its `pipeline_name`, so no two pipelines share a schema. This makes `drop_storage()` safe to call during `--full-refresh` without affecting other pipelines.

```python
from dlt.destinations.exceptions import DatabaseUndefinedRelation

if args.full_refresh:
    with pipeline.destination_client() as client:
        try:
            client.drop_storage()  # drops the pipeline's schema + all its tables
        except DatabaseUndefinedRelation:
            pass                   # schema doesn't exist yet — nothing to drop
    pipeline.drop()                # drops local pipeline state and pending packages

load_info = pipeline.run(source)
```

`drop_storage()` issues `DROP SCHEMA <dataset_name> CASCADE` with no `IF EXISTS` guard, hence the try/except. `pipeline.drop()` alone is insufficient because dlt restores resource state (e.g. `last_season`) from the destination's `_dlt_pipeline_state` table on the next run, causing incremental resources to resume from where they left off rather than from scratch.

## HTTP requests

Always set a `timeout` on `requests.get()`. Scraped sources (Baseball Reference, etc.) may rate-limit by holding the connection open rather than returning an error, causing the script to hang indefinitely without a timeout.

For web scraping (as opposed to data APIs), use the standard `requests` library directly rather than `dlt.sources.helpers.requests`. dlt's wrapper has built-in retry logic that silently retries timed-out requests, resetting the clock and making the timeout ineffective.

```python
import requests  # not from dlt.sources.helpers

response = requests.get(url, timeout=30)
```

## Yielding data from resources

Yield pyarrow tables, not Python dicts or polars DataFrames. dlt cannot JSON-serialize polars DataFrames and will raise `TypeError: Type is not JSON serializable: DataFrame`. Dicts work but are slow at scale — dlt normalizes them row-by-row in Python.

Pattern (see `loaders/statcast_dlt.py` and `loaders/retrosheet_game_logs_dlt.py`):

```python
import pyarrow as pa
import polars as pl

table = pl.read_csv(response.content, infer_schema_length=0).to_arrow()
```

## Arrow schema coercion

When yielding arrow tables, cast `large_utf8` → `utf8` and mark primary key columns as non-nullable. Without this, dlt logs a warning about schema hint mismatches on every run:

> "when merging arrow schema with dlt schema, several column hints were different"

Pattern:

```python
PRIMARY_KEYS = {'col_a', 'col_b'}

schema = pa.schema([
    (f.with_type(pa.utf8()) if f.type == pa.large_utf8() else f)
        .with_nullable(f.name not in PRIMARY_KEYS)
    for f in table.schema
])
table = table.cast(schema)
```

In practice, prefer `loaders.dlt_utils.to_arrow(df, primary_keys)`, which encapsulates this.
