# Interactive status-and-run tool

Bare `buehrle` launches an interactive **menu** that mirrors the top-level CLI: it fronts the loader **status grid** plus the utilities (`state`, `retrosheet-sync`, `install-chadwick`, `drop-db`). The status grid is one row per loader showing its last load and oldest table watermark, with two mutually-exclusive checkbox columns — **smart incremental** and **full refresh**; fill it once, submit, and the tool runs the selected loads. Choosing a utility runs it as a subprocess and returns to the menu (destructive ones, like `drop-db`, confirm first). `buehrle state` stays as the scriptable read-only report; `buehrle load <loader>` subcommands are unchanged.

## Considered options

- **UI technology.** Persistent full-screen [Textual](https://github.com/textualize/textual) app. An earlier iteration used a one-shot `prompt_toolkit` form, which was rejected once we wanted run output to stay *inside* the UI: streaming a subprocess into a one-shot form means dropping back to the terminal between actions. Textual's async event loop, screen stack, and widgets (`DataTable`, `OptionList`, `Log`, modal confirms) make the menu → grid → in-app output-pane flow natural, at the cost of a heavier dependency and an async/CSS paradigm. Stock `questionary` widgets were also rejected — a single checkbox list can't render a status table with two aligned per-row toggle columns.
- **Action semantics are watermark-driven.** *Smart incremental* loads from a loader's oldest table watermark (inclusive — cheap insurance against partially-loaded periods, and dlt merges idempotently) through today; a never-loaded table falls back to full-history. *Full refresh* always means a clean full rebuild (`--full-refresh --full-history`) — the only mode for single-shot loaders, and always warned as potentially slow. The "oldest across a loader's tables" rule fills the laggard table, since a loader runs once with a single scope.

## Consequences

- Each loader self-declares a `WATERMARKS` map (`{table: column}`); `state.py` collects them via the existing `LOADERS` list. Adding a loader requires declaring its watermarks but no edits to the interactive tool.
- Assumes a single loader's watermarked tables share one time dimension (they load under one scope), so the tool never compares a `season` against a `game_date` within a loader.
- The interactive code splits into a pure, tested `core.py` (resolver + planner), a tested `runner.py` (async subprocess streaming), and a coverage-omitted `app.py` (the Textual shell), to keep the `fail_under = 90` gate meaningful despite an untestable UI. The split also sidesteps Textual's `run_test` pilot, which can't settle while a subprocess streams — the IO logic is verified in `runner.py` against `python -c` jobs instead.
- Concurrency is a deliberate non-goal for v1; continue-on-error and per-loader output prefixing are in place to make it a clean later addition.

## Follow-ups (pending)

Surfaced while wiring up the status grid. Code is done; these are live-DB / data-quality chores.

- **Baseball-Reference schema migration.** The `war` loader's pipeline was renamed `baseball_reference` → `baseball_reference_war` so the two BR loaders own distinct schemas (and `war --full-refresh` can no longer `DROP SCHEMA baseball_reference CASCADE` over draft's data). The live DB still has the pre-rename layout, so:
  - [ ] Re-run war into its new schema: `buehrle load baseball-reference-war --full-refresh`.
  - [ ] Re-run draft into its schema: `buehrle load baseball-reference-draft-results --full-history --full-refresh`.
  - [ ] Drop the now-orphaned schema: `DROP SCHEMA baseball_reference CASCADE`.
### Loaders reporting no watermark

A blank `watermark` in `buehrle state` is a useful signal, not just an absence — it
falls into four buckets, and three of them are actionable. (`oldest` collapses to
`None` whenever *any* of a loader's declared tables is absent or empty, by design.)

- **By design — full-refresh-only.** `baseball_reference_war`, `lahman`,
  `chadwick_register` declare `WATERMARKS = {}` (single-shot `replace` loaders).
  Blank is correct; the grid offers only full refresh. No action.
- **Never loaded.** `baseball_reference_draft` and `retrosheet` (game_logs) have no
  tables in the live DB yet. They'll populate once loaded (draft is covered by the
  migration above; game_logs just needs a first `buehrle load retrosheet-game-logs` run).
- [ ] **Partially loaded — the signal is doing its job.** These have only some of
  their declared tables, so the watermark is (correctly) suppressed until a full
  load makes them current:
  - `statcast_{batting,fielding,pitching,running}_leaderboards` — each schema holds
    only **1** of its 5–8 declared tables. Backfill the rest (`--full-history`).
  - `retrosheet_events` — `retrosheet_game_logs_box` is **empty (0 rows)** while
    `_full` (2025) and `_deduced` (1968) are populated. Decide whether `box` ever
    yields rows: if it's structurally always-empty, drop it from `WATERMARKS`;
    otherwise treat the gap as a load to fill.
- [x] **Data bug — `retrosheet_schedules`.** `schedules.date` contained a literal
  `'Date'` value (a CSV header leaked in as data), so `MAX(date)` returned `'Date'`.
  Root cause was reading every file with `has_header=False`; 146/149 season files
  carry a `Date,Num,Day,...` header. Fixed by reading headerless and stripping the
  header row by content (duplicate `League`/`Game` names rule out `has_header=True`).
  Uncovered a second latent bug: 2024+ files add a 13th `Location` column, which the
  fixed-width `COLUMNS` list silently mislabeled. Now normalised to one canonical
  13-column schema (with `location`), null-filling it for the older 12-column format.
  - [ ] Reload to apply: `buehrle load retrosheet-schedules --full-history --full-refresh`.
