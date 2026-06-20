# Buehrle

Loaders for public baseball data sources into a local DuckDB database.

Each source has a Python loader under [loaders/](loaders/) that uses [dlt](https://dlthub.com/) to land data into [data/buehrle-raw.duckdb](data/).

## Sources

| Source | Loader | What it loads |
|---|---|---|
| MLB Stats API | [loaders/mlb_statsapi/](loaders/mlb_statsapi/) | Schedules (per-game + line-score innings + umpires) |
| FanGraphs | [loaders/fangraphs/](loaders/fangraphs/) | Season leaderboards (bat / pit / fld) |
| Statcast | [loaders/statcast/](loaders/statcast/) | Pitch-by-pitch + Savant leaderboards |
| Baseball Reference | [loaders/baseball_reference/](loaders/baseball_reference/) | bWAR, draft results |
| Retrosheet | [loaders/retrosheet/](loaders/retrosheet/) | Events, game logs, schedules, rosters, umpires |
| Lahman | [loaders/lahman/](loaders/lahman/) | Full Lahman release (from local CSVs) |
| Chadwick Bureau | [loaders/chadwick/](loaders/chadwick/) | Player ID crosswalk |

A dbt project lives under [buehrle_dbt/](buehrle_dbt/) and reads from the loader-produced schemas. It is not documented yet.

## Docs

- [Getting started](docs/getting_started.md) — install, prerequisites, first run
- [Loaders](docs/loaders.md) — CLI conventions, per-loader reference, refresh patterns
- [MLB Stats API](docs/mlb_statsapi.md) — endpoint notes for the schedules loader
- [FanGraphs column naming](docs/fangraphs_column_naming.md) — column-normalization rules for FanGraphs
