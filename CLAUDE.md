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
    f.with_type(pa.utf8()).with_nullable(f.name not in PRIMARY_KEYS)
    if f.type == pa.large_utf8()
    else f
    for f in table.schema
])
table = table.cast(schema)
```
