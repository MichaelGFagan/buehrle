# Getting started

## Install

Python 3.12+. The project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync
```

This installs everything declared in [pyproject.toml](../pyproject.toml), including dlt with the DuckDB destination.

## Where data lands

All loaders write to a single DuckDB file at [data/buehrle-raw.duckdb](../data/). Each loader uses its own schema (`dataset_name`) inside that database, equal to its `pipeline_name` — e.g. the MLB Stats API schedules loader writes to schema `mlb_statsapi_schedules`. This isolation is what makes `--full-refresh` on one loader safe to run without affecting any other loader's tables.

The path is set in [loaders/dlt_utils.py](../loaders/dlt_utils.py) via `DB_PATH`. Edit that constant to point elsewhere.

## Per-source prerequisites

Most loaders just hit an HTTP endpoint and need nothing on disk. Three exceptions:

### Lahman

The Lahman database is distributed by SABR on Box — there's no scriptable URL. Download the annual release manually and unzip into [data/lahman/](../data/lahman/) so the directory contains the CSVs the loader expects (filenames are hardcoded in [loaders/lahman/lahman.py](../loaders/lahman/lahman.py); a missing or renamed file will fail loudly).

### Retrosheet

Retrosheet loaders read from a local clone of the Chadwick Bureau retrosheet repo. Set this up once:

```bash
buehrle retrosheet-sync
```

This clones (or pulls) `chadwickbureau/retrosheet` into [data/retrosheet/](../data/retrosheet/). Re-run when you want a fresh pull.

The events loader additionally needs the Chadwick `cwtools` binaries (`cwevent`, etc.) on `PATH`:

```bash
buehrle install-chadwick
```

This is a thin wrapper around `brew install chadwick`.

## First run

After `uv sync`, the `buehrle` command is on `PATH`. Bare `buehrle` (no subcommand) launches the interactive [menu](loaders.md#2-interactive-cli) — the loader status grid plus the utilities (show state, sync Retrosheet, install Chadwick, drop the database). `buehrle --help` lists the top-level commands (`load` plus utilities); `buehrle load --help` lists every loader; `buehrle load <loader> --help` shows that loader's flags.

The simplest end-to-end smoke test is the MLB Stats API schedules loader — no local data, fast endpoint:

```bash
buehrle load mlb-statsapi-schedules
```

With no args this loads the current season's schedule (~2,900 games for a recent year, one HTTP call). On success you'll see a dlt load summary and [data/buehrle-raw.duckdb](../data/) will contain a `mlb_statsapi_schedules` schema with three tables.

## Querying the data

Any DuckDB client works. From Python:

```python
import duckdb
duckdb.connect('data/buehrle-raw.duckdb').sql(
    'SELECT * FROM mlb_statsapi_schedules.schedules LIMIT 5'
)
```

Or from the CLI: `duckdb data/buehrle-raw.duckdb`.
