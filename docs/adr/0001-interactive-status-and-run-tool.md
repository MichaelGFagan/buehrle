# Interactive status-and-run tool

Bare `buehrle` launches an interactive **status grid**: one row per loader showing its last load and oldest table watermark, with two mutually-exclusive checkbox columns — **smart incremental** and **full refresh**. Fill it once, submit, and the tool runs the selected loads and exits. `buehrle state` stays as the scriptable read-only report; `buehrle <loader>` subcommands are unchanged.

## Considered options

- **UI technology.** Hand-built one-shot [`prompt_toolkit`](https://github.com/prompt-toolkit/python-prompt-toolkit) form. Rejected: a persistent full-screen TUI (Textual) — heavier dependency, async/CSS paradigm shift, and live-refresh/concurrent-run machinery we don't need yet; and stock `questionary` widgets — a single checkbox list can't render a status table with two aligned per-row toggle columns. `prompt_toolkit` is already a transitive dependency and gives full layout control while staying one-shot.
- **Action semantics are watermark-driven.** *Smart incremental* loads from a loader's oldest table watermark (inclusive — cheap insurance against partially-loaded periods, and dlt merges idempotently) through today; a never-loaded table falls back to full-history. *Full refresh* always means a clean full rebuild (`--full-refresh --full-history`) — the only mode for single-shot loaders, and always warned as potentially slow. The "oldest across a loader's tables" rule fills the laggard table, since a loader runs once with a single scope.

## Consequences

- Each loader self-declares a `WATERMARKS` map (`{table: column}`); `state.py` collects them via the existing `LOADERS` list. Adding a loader requires declaring its watermarks but no edits to the interactive tool.
- Assumes a single loader's watermarked tables share one time dimension (they load under one scope), so the tool never compares a `season` against a `game_date` within a loader.
- The interactive code splits into a pure, tested `core.py` (resolver + planner) and a coverage-omitted `app.py` (the `prompt_toolkit` shell), to keep the `fail_under = 90` gate meaningful despite an untestable UI.
- Concurrency is a deliberate non-goal for v1; continue-on-error and per-loader output prefixing are in place to make it a clean later addition.

## Follow-ups (pending)

Surfaced while wiring up the status grid. Code is done; these are live-DB / data-quality chores.

- **Baseball-Reference schema migration.** The `war` loader's pipeline was renamed `baseball_reference` → `baseball_reference_war` so the two BR loaders own distinct schemas (and `war --full-refresh` can no longer `DROP SCHEMA baseball_reference CASCADE` over draft's data). The live DB still has the pre-rename layout, so:
  - [ ] Re-run war into its new schema: `buehrle baseball-reference-war --full-refresh`.
  - [ ] Re-run draft into its schema: `buehrle baseball-reference-draft-results --full-history --full-refresh`.
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
  migration above; game_logs just needs a first `buehrle retrosheet-game-logs` run).
- [ ] **Partially loaded — the signal is doing its job.** These have only some of
  their declared tables, so the watermark is (correctly) suppressed until a full
  load makes them current:
  - `statcast_{batting,fielding,pitching,running}_leaderboards` — each schema holds
    only **1** of its 5–8 declared tables. Backfill the rest (`--full-history`).
  - `retrosheet_events` — `retrosheet_game_logs_box` is **empty (0 rows)** while
    `_full` (2025) and `_deduced` (1968) are populated. Decide whether `box` ever
    yields rows: if it's structurally always-empty, drop it from `WATERMARKS`;
    otherwise treat the gap as a load to fill.
- [ ] **Data bug — `retrosheet_schedules`.** `schedules.date` contains a literal
  `'Date'` value (a CSV header leaked in as data), so `MAX(date)` returns `'Date'`.
  Strip/filter the header row on load, then reload.
