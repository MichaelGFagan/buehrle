# Loaders

Each loader is a Python script under `loaders/<source>/` runnable as a `buehrle` subcommand — e.g. `buehrle load fangraphs --season 2025`. Run `buehrle --help` for the full list and `buehrle load <loader> --help` for per-loader flags. All loaders write to [data/buehrle-raw.duckdb](../data/), each into its own schema. See [Getting started](getting_started.md) for setup.

## CLI conventions

Most loaders accept a standard set of scope flags. The rule: **no args defaults to a small, recent scope** (the current season) so an accidental run cannot hammer a source or produce a huge load. Backfills are opt-in.

### Scope flags

| Flag | Meaning |
|---|---|
| *(no args)* | Current season only |
| `--season YYYY` | One specific season |
| `--start-season YYYY --end-season YYYY` | A season range (inclusive). Both required if either is set. |
| `--full-history` | Earliest available season through current. Mutually exclusive with `--season` / range flags. |

A few loaders ([mlb_statsapi/schedules.py](../loaders/mlb_statsapi/schedules.py), [statcast/statcast_pitches.py](../loaders/statcast/statcast_pitches.py)) also accept date flags:

| Flag | Meaning |
|---|---|
| `--date YYYY-MM-DD` | One specific date |
| `--start-date / --end-date` | A date range (inclusive). Both required if either is set. |

Date flags are mutually exclusive with season flags.

### `--resources`

Multi-resource loaders accept `--resources NAME [NAME ...]` to load a subset of their tables. Default loads all. Unknown names error out with the list of available resources.

| Loader | Resources |
|---|---|
| [mlb_statsapi/schedules.py](../loaders/mlb_statsapi/schedules.py) | `schedules`, `schedules_linescore_innings`, `schedules_officials` |
| [fangraphs/fangraphs.py](../loaders/fangraphs/fangraphs.py) | `batting`, `pitching`, `fielding` |
| [baseball_reference/baseball_reference_war.py](../loaders/baseball_reference/baseball_reference_war.py) | `war_batting`, `war_pitching` |
| [lahman/lahman.py](../loaders/lahman/lahman.py) | 27 tables — run with `--help` or see [lahman.py](../loaders/lahman/lahman.py) `TABLES` |
| [statcast/statcast_batting_leaderboards.py](../loaders/statcast/statcast_batting_leaderboards.py) | `exit_velo_barrels`, `expected_stats`, `percentile_ranks`, `pitch_arsenal_stats`, `bat_tracking` |
| [statcast/statcast_pitching_leaderboards.py](../loaders/statcast/statcast_pitching_leaderboards.py) | `exit_velo_barrels`, `expected_stats`, `percentile_ranks`, `pitch_arsenals`, `pitch_arsenal_stats`, `pitch_movement`, `active_spin`, `bat_tracking` |
| [statcast/statcast_fielding_leaderboards.py](../loaders/statcast/statcast_fielding_leaderboards.py) | `outs_above_average`, `fielding_run_value`, `outfield_directional_oaa`, `outfield_catch_prob`, `outfielder_jump`, `catcher_poptime`, `catcher_framing` |
| [statcast/statcast_running_leaderboards.py](../loaders/statcast/statcast_running_leaderboards.py) | `sprint_speed`, `running_splits` |

For [mlb_statsapi/schedules.py](../loaders/mlb_statsapi/schedules.py), the API fetch is shared across all three resources and runs regardless of `--resources` — the flag only narrows what gets written to the destination.

### `--full-refresh`

Orthogonal to scope: drops the loader's schema before loading, so the next run rebuilds it from scratch. The two flag families compose:

|  | Default `--full-refresh` off | `--full-refresh` on |
|---|---|---|
| Default scope | Incremental refresh of current season | Wipe schema, load current season fresh |
| `--full-history` | Backfill all seasons, merged into existing | Wipe schema, full backfill from scratch |

For a clean first-time backfill: `--full-history --full-refresh`. For ongoing incremental updates: no args.

## Per-loader reference

### MLB Stats API

| Script | Loads | Default | Write disposition |
|---|---|---|---|
| [mlb_statsapi/schedules.py](../loaders/mlb_statsapi/schedules.py) | Per-game schedule + line-score innings + umpires | Current season | `merge` on `game_pk` (and child PKs) |

`--full-history` backfills to 1876 per the MLB seasons endpoint. See [mlb_statsapi.md](mlb_statsapi.md) for endpoint detail.

### FanGraphs

| Script | Loads | Default | Write disposition |
|---|---|---|---|
| [fangraphs/fangraphs.py](../loaders/fangraphs/fangraphs.py) | Season leaderboards (bat / pit / fld) | Current season | `merge` on `(playerid, Season, Team, leg)` |

Column naming is non-trivial — see [fangraphs_column_naming.md](fangraphs_column_naming.md).

### Statcast

| Script | Loads | Default | Write disposition |
|---|---|---|---|
| [statcast/statcast_pitches.py](../loaders/statcast/statcast_pitches.py) | Pitch-by-pitch | Current season (date range) | `merge` on `(game_pk, at_bat_number, pitch_number)` |
| [statcast/statcast_batting_leaderboards.py](../loaders/statcast/statcast_batting_leaderboards.py) | Savant batting leaderboards | Current season | `merge` per resource |
| [statcast/statcast_pitching_leaderboards.py](../loaders/statcast/statcast_pitching_leaderboards.py) | Savant pitching leaderboards | Current season | `merge` per resource |
| [statcast/statcast_fielding_leaderboards.py](../loaders/statcast/statcast_fielding_leaderboards.py) | Savant fielding leaderboards (OAA, framing, etc.) | Current season | `merge` per resource |
| [statcast/statcast_running_leaderboards.py](../loaders/statcast/statcast_running_leaderboards.py) | Sprint speed, running splits | Current season | `merge` per resource |

`statcast_pitches` is stateful: it stores `last_date` per resource and resumes from there on the next run rather than re-fetching the whole season. Pass `--update` to force a re-fetch of the full requested range.

### Baseball Reference

| Script | Loads | Default | Write disposition |
|---|---|---|---|
| [baseball_reference/baseball_reference_war.py](../loaders/baseball_reference/baseball_reference_war.py) | bWAR for batters and pitchers | All-time, full file | `replace` |
| [baseball_reference/baseball_reference_draft_results.py](../loaders/baseball_reference/baseball_reference_draft_results.py) | MLB draft results | Current season | `merge` on `(Year, draft_type, Rnd, RdPck)` |

`baseball_reference_war.py` has no season scope, just `--full-refresh`. Each run re-scrapes and replaces the WAR tables wholesale.

### Retrosheet

| Script | Loads | Default | Write disposition |
|---|---|---|---|
| [retrosheet/retrosheet_events.py](../loaders/retrosheet/retrosheet_events.py) | Play-by-play (via `cwevent`) | Current season | `merge` on `(GAME_ID, EVENT_ID)` |
| [retrosheet/retrosheet_game_logs.py](../loaders/retrosheet/retrosheet_game_logs.py) | Game logs | Current season | `merge` on `(home_team, date, game_num)` |
| [retrosheet/retrosheet_schedules.py](../loaders/retrosheet/retrosheet_schedules.py) | Season schedules | Current season | `merge` on `(date, game_num, home_team)` |
| [retrosheet/retrosheet_rosters.py](../loaders/retrosheet/retrosheet_rosters.py) | Team rosters | Current season | `merge` on `(player_id, team, season)` |
| [retrosheet/retrosheet_umpires.py](../loaders/retrosheet/retrosheet_umpires.py) | Umpire assignments | Current season | `merge` on `(umpire_id, season)` |

All read from the local clone at [data/retrosheet/](../data/retrosheet/) — run [retrosheet_sync.py](../loaders/retrosheet/retrosheet_sync.py) first.

### Lahman

| Script | Loads | Default | Write disposition |
|---|---|---|---|
| [lahman/lahman.py](../loaders/lahman/lahman.py) | Full Lahman release | All CSVs in [data/lahman/](../data/lahman/) | `replace` per table |

No scope flags. The loader replaces every Lahman table from the local CSVs. To update, re-download the SABR release into [data/lahman/](../data/lahman/) and re-run.

### Chadwick Bureau register

| Script | Loads | Default | Write disposition |
|---|---|---|---|
| [chadwick/chadwick_register.py](../loaders/chadwick/chadwick_register.py) | Player ID crosswalk (MLBAM, BREF, FG, Retro, etc.) | Full file | `replace` on `key_uuid` |

Single-shot scrape — `--full-refresh` only.

## Refresh patterns

### Bringing data up to date (incremental)

For most loaders, **no args** is the incremental refresh — it re-pulls the current season and merges by primary key, so any rows added or changed upstream land in the destination and nothing is dropped:

```bash
buehrle load mlb-statsapi-schedules
buehrle load fangraphs
buehrle load statcast-pitches
buehrle load statcast-batting
# ...etc
```

The three `replace` loaders ([baseball_reference_war.py](../loaders/baseball_reference/baseball_reference_war.py), [chadwick_register.py](../loaders/chadwick/chadwick_register.py), [lahman/lahman.py](../loaders/lahman/lahman.py)) re-pull fully on every run — for them, no args is always a fresh pull.

Two important exceptions where no-args **will not** work mid-season:

- **Retrosheet** publishes after a season ends (usually the following winter). Running any `retrosheet_*` loader with no args during an active season will 404 because the source files for the current year don't exist yet. Use `--season <last_complete_year>`.
- **Baseball Reference draft results** defaults to the current season, but the MLB draft happens in July. Running before then yields no data and logs scrape failures. Use `--season <last_completed_draft_year>` until the current draft has happened.

### Clean first-time backfill

```bash
buehrle load <loader> --full-history --full-refresh
```

For loaders without season scope, drop `--full-history`:

```bash
buehrle load baseball-reference-war --full-refresh
buehrle load chadwick-register --full-refresh
buehrle load lahman --full-refresh
```

## Planned improvements

Forward-looking work on the loader layer — this is the immediate roadmap, in priority order. Each step builds on the previous one.

### ✅ Done: Per-table `_dlt_load_id`

Every loaded table now carries `_dlt_load_id` so per-table provenance is queryable from the destination (see [loaders/state.py](../loaders/state.py) and `buehrle state --mode table`). Arrow-yielding resources used to skip this column by default — combined with à la carte `--resources`, the schema-level `_dlt_loads` table couldn't tell which resources within a schema were fresh vs. stale.

Enabled via [.dlt/config.toml](../.dlt/config.toml):

```toml
[normalize.parquet_normalizer]
add_dlt_load_id = true
add_dlt_id = false
```

`add_dlt_id` stays off — it's regenerated per load for arrow data (non-deterministic) and high-cardinality, so it costs storage without giving stable row identity. Note dlt resolves `.dlt/config.toml` relative to the **current working directory**, so run `buehrle` from the repo root.

**Migration caveat (one-time, per pipeline):** enabling the flag only affects loads from this point on. A table created *before* the flag lacks the column, and dlt's attempt to `ALTER TABLE ADD COLUMN _dlt_load_id` on it fails — DuckDB can't add a constrained column to a populated table (`Parser Error: Adding columns with constraints not yet supported`), which aborts the load and leaves a pending package. The fix is to **`--full-refresh` each pipeline once** so the table is recreated with the column from the start (this is the normal first-backfill action anyway). Until a given source is full-refreshed, its arrow tables will error on the next incremental run. Dict-yielding tables are unaffected — they always carried `_dlt_load_id` regardless of this flag.

### 1. Standardize how a new loader is added

A loader is now a prescribed shape rather than a from-scratch script. The shared pieces live in [loaders/cli.py](../loaders/cli.py) (arg wiring, scope resolution, resource selection, the run tail) and [loaders/dlt_utils.py](../loaders/dlt_utils.py) (pipeline construction, full-refresh, arrow coercion). Per-loader code is reduced to: define the `@dlt.source`, wire args in `register()`, resolve scope + build the source in `main()`, then hand off to `run_loader`.

The runtime tail every loader used to repeat verbatim —

```python
source = apply_resources(source, args)
if args.full_refresh:
    handle_full_refresh(pipeline)
load_info = pipeline.run(source)
print(load_info)
```

— is now a single call: `run_loader(pipeline, source, args)` (see [loaders/cli.py](../loaders/cli.py)). It applies `--resources` filtering and `--full-refresh` when the loader defines those flags and no-ops otherwise, so the same call works for every loader regardless of which scope/refresh flags it exposes.

**Still open:** the `register()` boilerplate (`add_parser` → `add_*_args` → `--full-refresh` → `add_resources_arg` → `set_defaults`) remains explicit per loader. It's deliberately left un-factored — collapsing it into a declarative spec risks more indirection than it saves, and the interactive CLI (step 2) needs per-loader arg metadata anyway, which a spec would have to expose.

The interactive CLI (step 2) needs to discover available loaders and their resources programmatically; this consistent loader shape makes that discovery layer trivial instead of a special case per source.

### 2. Interactive CLI

The flat `buehrle load <loader>` entrypoint exists today (see [loaders/__main__.py](../loaders/__main__.py)). What's still planned is an interactive layer on top that walks the user through loader → resources → scope → refresh-mode, instead of requiring them to remember the right subcommand and flags per source. Useful for the common "I want to update X and Y but not Z" workflow that's currently several invocations.
