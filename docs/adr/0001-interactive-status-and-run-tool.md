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
