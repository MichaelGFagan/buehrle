# Buehrle

Loaders that land public baseball data sources into a local DuckDB database, with a dbt project on top. This glossary defines the shared language for the project's data-loading and status tooling.

## Language

**Loader**:
A module that ingests one source (or one slice of a source) into its own DuckDB schema via dlt, exposed as a `buehrle <loader>` subcommand.
_Avoid_: importer, scraper, job

**Source**:
An external data provider (FanGraphs, Statcast, Retrosheet, etc.). One source may be served by several loaders.
_Avoid_: feed, provider

**Watermark**:
The high-water mark of a loaded table — the latest data point it contains, expressed in that table's own time dimension (e.g. a `season` for FanGraphs leaderboards, a `game_date` for Statcast pitches). Declared per table; tables with no time dimension have none.
_Avoid_: high-water mark, latest, cursor

**Last load**:
When a loader's data was most recently written, taken from dlt's `_dlt_loads` ledger. Distinct from the watermark: last load is *when we ran*, the watermark is *how current the data is*.
_Avoid_: last run, freshness

**Status view**:
The read-only report of every loader's last load and table watermarks (today's `buehrle state`).
_Avoid_: dashboard, report

**Status grid**:
The interactive form shown by bare `buehrle`: one row per loader with its last load and oldest watermark, plus two mutually-exclusive checkbox columns (smart incremental / full refresh). A one-shot form — fill it once, submit, and the tool runs the selected loads and exits.
_Avoid_: TUI, dashboard, menu

**Smart incremental**:
A run mode that brings a loader current by loading from its oldest table watermark (inclusive) through today. On a never-loaded table it falls back to a full backfill (with a warning).
_Avoid_: catch-up, sync, update

**Full refresh** (as a status-grid action):
A run mode that drops the loader's schema and rebuilds it from the full history (`--full-refresh --full-history`). The only available mode for single-shot loaders. Always warned as potentially slow.
_Avoid_: rebuild, reload, reset
