# Plan: DuckLake (Postgres-backed) as a second loader destination, alongside the .duckdb file

## Context

Loaders all write to one DuckDB file (`data/buehrle-raw.duckdb`), which is
single-writer / single-process and therefore blocks concurrent loads. DuckLake
(catalog in Postgres + Parquet data files) gives snapshot-isolated concurrent
writers with far less machinery than an Iceberg REST catalog.

Goal for this cut: make DuckLake an **opt-in** second destination selected at
runtime, leaving the `.duckdb` path unchanged as the default. `buehrle state` and
the Textual grid must read whichever backend is active, and `drop-db` must be
able to reset either backend.

Decisions locked in:
- **Catalog:** local Postgres via Docker (new `docker-compose.yml`) on port
  **5432**. Verified now: nothing is listening on 5432 (the Homebrew
  `postgresql@15` service is registered but stopped/errored), so Docker binds
  5432 directly. This was a one-time check — no per-run preflight.
- **Switch:** env var `BUEHRLE_BACKEND` (`duckdb` default | `ducklake`),
  mirroring the existing `BUEHRLE_DB` pattern.
- **Scope:** loader write path + `state`/status read path + backend-aware
  `drop-db`. dbt-on-DuckLake is a deliberate follow-on.

Key facts confirmed during exploration:
- dlt 1.24 ships a native `ducklake` destination (`dlt.destinations.ducklake`).
  `DuckLakeCredentials` takes `catalog` (a Postgres connection string) +
  `storage` (a data-files path) + `ducklake_name`, and performs the
  `ATTACH 'ducklake:postgres:...'` internally.
- The write path funnels through **one** function: `make_pipeline()` in
  `loaders/dlt_utils.py`. Every loader calls it. `dataset_name=name` becomes a
  schema inside the shared DuckLake catalog, so loaders stay schema-separated —
  which is what makes concurrent loads safe.
- Reads go through `duckdb.connect(..., read_only=True)` in `loaders/state.py`
  and `loaders/interactive/app.py`, then portable `information_schema` /
  `"schema"."table"` queries.
- `loader_status()` already handles absent schemas/tables gracefully (never-loaded
  rows), so an empty DuckLake catalog renders correctly without special-casing.
- `DuckLakeSqlClient` exists, so `handle_full_refresh()`'s `drop_storage()`
  (DROP SCHEMA CASCADE) should work unchanged — verify during implementation.
- Docker is available. Existing tests mock `make_pipeline` (conftest
  `fake_make_pipeline`) and assert on `resolve_db_path`, so keeping `duckdb` as
  the default branch preserves them.

## Approach

Add a `ducklake` branch behind `BUEHRLE_BACKEND`. The DuckDB-file path stays the
default and is untouched when the var is unset. All backend selection lives in
`dlt_utils.py`; loaders, `cli.py`, and `run_loader` need **no** changes.

### 1. Infra — `docker-compose.yml` (new, repo root)
- `postgres:16`, database `buehrle_ducklake`, user/pass `buehrle`/`buehrle`, host
  port **5432**→5432, named volume for persistence.
- Parquet data files live under `data/ducklake/` (gitignored), not in Postgres.

### 2. Config helpers — `loaders/dlt_utils.py`
- `resolve_backend() -> str`: reads `BUEHRLE_BACKEND`, default `'duckdb'`.
- `ducklake_catalog_dsn() -> str`: reads `BUEHRLE_DUCKLAKE_CATALOG`, default
  `postgresql://buehrle:buehrle@localhost:5432/buehrle_ducklake`.
- `ducklake_storage_path() -> str`: reads `BUEHRLE_DUCKLAKE_STORAGE`, default
  `<repo>/data/ducklake/`.
- Keep `resolve_db_path()` / `DEFAULT_DB_PATH` exactly as-is (tests depend on them).

### 3. Destination selection — `make_pipeline()` in `loaders/dlt_utils.py`
Branch on `resolve_backend()`:
- `'duckdb'` (default): unchanged — `dlt.destinations.duckdb(resolve_db_path())`.
- `'ducklake'`:
  `dlt.destinations.ducklake(credentials=DuckLakeCredentials(catalog=ducklake_catalog_dsn(), storage=ducklake_storage_path()))`,
  still `dataset_name=name`.
- `handle_full_refresh()` stays as-is; verify `drop_storage()` on the ducklake
  client, fall back to `DROP SCHEMA "<dataset>" CASCADE` if it raises.

### 4. Backend-aware read connection — `loaders/dlt_utils.py`
Add one shared helper so state + TUI don't duplicate attach logic:
- `open_read_connection() -> duckdb.DuckDBPyConnection`:
  - `duckdb`: `duckdb.connect(resolve_db_path(), read_only=True)` (today's behavior).
  - `ducklake`: `duckdb.connect()` → `INSTALL/LOAD ducklake, postgres` →
    `ATTACH 'ducklake:postgres:...' AS ducklake (DATA_PATH '<storage>', READ_ONLY); USE ducklake;`.

### 5. Status read path — `loaders/state.py`
- Route the connection through `open_read_connection()` instead of the direct
  `duckdb.connect(str(args.db), read_only=True)` (state.py:192).
- `--db` / `DEFAULT_DB` stay meaningful for the duckdb backend; ignored (with a
  `--help` note) under ducklake. Queries unchanged.

### 6. Textual grid — `loaders/interactive/app.py`
- Change `load_rows()` (app.py:58) to use `open_read_connection()` and make the
  "nothing loaded yet" short-circuit backend-aware: the current `db.exists()`
  file check is meaningless for DuckLake, so for ducklake always open the catalog
  and let `loader_status()` yield never-loaded rows for an empty catalog.
- Update `run()` / `DEFAULT_DB` resolution (app.py:341-343) to match.
- Result: the grid reads DuckDB or DuckLake status identically. (app.py is
  coverage-exempt but must still work.)

### 7. Backend-aware `drop-db` — `loaders/drop_db.py` (reworked)
- New `--target {duckdb,ducklake,both}`, defaulting to the active
  `BUEHRLE_BACKEND`. Keep `--yes` confirmation; the prompt names exactly what
  will be destroyed.
- `duckdb`: delete the `.duckdb` file (+ `.wal`) — current behavior.
- `ducklake` (**native teardown**): connect + attach the catalog, `DROP SCHEMA
  "<loader>" CASCADE` for every loader schema present, then clear the
  `data/ducklake/` storage directory — the DuckLake analog of deleting the file.
  `--help` also notes `docker compose down -v` as the full nuke (drops the
  Postgres volume too).
- `both`: DuckDB file deletion **and** DuckLake teardown.
- Update the Taskfile `drop-db` desc and add `task drop-db -- --target both`
  guidance.

### 8. Dependencies — `pyproject.toml`
- Expect **no new Python dependency**: the Postgres catalog is attached through
  DuckDB's `postgres` extension (auto-installed at runtime) and the `ducklake`
  destination ships with `dlt`. Confirm during verification; add a driver only if
  an attach actually fails.

### 9. Docs / conventions (small)
- Document `BUEHRLE_BACKEND` + the two DuckLake env vars + `docker compose up` in
  `README.md`/`CONTEXT.md`; note the env var in `CLAUDE.md` next to `BUEHRLE_DB`.
- Add `data/ducklake/` to `.gitignore`.

## Files to modify / add
- `docker-compose.yml` — **new**
- `loaders/dlt_utils.py` — backend resolution, ducklake destination, `open_read_connection()`
- `loaders/state.py` — use `open_read_connection()`
- `loaders/interactive/app.py` — backend-aware `load_rows()` + entry point
- `loaders/drop_db.py` — `--target duckdb|ducklake|both`, native DuckLake teardown
- `Taskfile.yml` — `drop-db` desc/args
- `.gitignore`, `README.md`/`CONTEXT.md`, `CLAUDE.md` — docs
- `pyproject.toml` — only if a driver proves necessary

## Verification
1. `docker compose up -d`; confirm Postgres healthy on 5432 and `buehrle_ducklake`
   exists.
2. `BUEHRLE_BACKEND=ducklake uv run buehrle load lahman --full-refresh`; confirm
   Parquet files under `data/ducklake/` and schema/tables/row-counts via a duckdb
   attach.
3. `BUEHRLE_BACKEND=ducklake uv run buehrle state` shows a last-load time; launch
   bare `buehrle` and confirm the **TUI grid** populates from DuckLake.
4. Concurrency (the goal): run two loaders in parallel against DuckLake (e.g.
   `lahman` + `chadwick`); both commit without a lock error.
5. `drop-db`: `BUEHRLE_BACKEND=ducklake uv run buehrle drop-db --yes` clears
   DuckLake; `--target both` clears both; default duckdb path still deletes the file.
6. Default backend untouched: `task smoke -- mlb-statsapi-schedules --date
   2025-04-01` still uses a throwaway `.duckdb`; `task check` (lint + full
   coverage suite) passes.

## Out of scope (follow-ups)
- dbt `ducklake` target in `~/.dbt/profiles.yml` (also needs the raw-vs-transformed
  split sorted: loaders write `buehrle-raw.duckdb`, dbt reads `buehrle.duckdb`).
- DuckLake-native reset beyond `drop-db` (e.g. a dedicated compact/expire task).
- Postgres service container for CI if DuckLake loads ever run there.
