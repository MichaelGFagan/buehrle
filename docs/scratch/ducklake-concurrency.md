# Plan: Run loaders concurrently (DuckLake lane)

## Context

Today buehrle runs selected loaders **one at a time**. The TUI's "press r to
run" flow drives `stream_jobs()` (`loaders/interactive/runner.py:40`), a
`for job in jobs` loop that spawns `python -m loaders load <loader> …` and
blocks on `proc.wait()` before starting the next. Sequential execution is not a
UI choice — it's forced by the default DuckDB backend, a single-file,
single-writer database. Two loader processes writing that file at once collide
on its write lock.

The write-concurrency blocker is **already solved** by the opt-in DuckLake
backend in `loaders/dlt_utils.py` (`BUEHRLE_BACKEND=ducklake`): a Postgres
catalog + Parquet data files giving snapshot-isolated concurrent writers. Each
loader already writes its own schema (`dataset_name=name`), so parallel loaders
never touch the same tables and never conflict at commit.

So the only work left is the **execution layer**: parallelize the runner, but
**only on the DuckLake backend**. The DuckDB path stays sequential and
unchanged — it physically cannot do otherwise. Goal: concurrent runs in both
the TUI grid and a new CLI batch command, with a bounded, configurable worker
count.

## Approach

Gate concurrency to `ducklake` with a single guard, share the per-job
subprocess/streaming logic between the sequential and parallel paths, and keep
the DuckDB path byte-for-byte what it is today.

### 1. Concurrency config — `loaders/interactive/core.py`
`core.py` is the pure, tested logic module — the right home for the
worker-count decision (no I/O, no subprocess).

- `resolve_max_concurrency() -> int`: read `BUEHRLE_MAX_CONCURRENCY`, default
  `4`, clamp to `>= 1`. Mirrors the existing `resolve_*` env pattern in
  `dlt_utils.py` (read at call time).
- `worker_count(backend: str, selected: int, cap: int) -> int`: return `1` when
  `backend == 'duckdb'` (the safety gate) or `selected <= 1`; otherwise
  `min(cap, selected)`. This is the whole gating rule, in one testable function.

### 2. Runner — `loaders/interactive/runner.py`
- Extract the per-job body (spawn `subprocess.Popen`, stream `\r`-normalised,
  `[label]`-prefixed lines, return success bool) from `stream_jobs` into a
  private `_run_one_job(job, write, base_argv, env) -> bool`.
- Keep **`stream_jobs` exactly as-is** (sequential loop over `_run_one_job`) so
  its current test and the DuckDB path are untouched.
- Add `stream_jobs_concurrent(jobs, write, *, max_workers, base_argv=...)`: a
  `concurrent.futures.ThreadPoolExecutor(max_workers)` submitting one
  `_run_one_job` per job, collecting failures from the futures. `write` must be
  thread-safe — it already is on both call sites (TUI uses
  `call_from_thread`; tests use `list.append`, atomic under the GIL). Emit the
  same closing `Done: ok/total` / `Failed: …` summary as `stream_jobs`.
- Add a thin dispatcher `run_jobs(jobs, write, *, max_workers, base_argv=...)`:
  `max_workers <= 1` → `stream_jobs`; else `stream_jobs_concurrent`.

### 3. TUI wiring — `loaders/interactive/app.py`
- In `RunScreen._run_jobs` (app.py:120), replace `stream_jobs(self._jobs, write)`
  with `run_jobs(self._jobs, write, max_workers=worker_count(resolve_backend(),
  len(self._jobs), resolve_max_concurrency()))`.
- In `action_run` (app.py:245) intro text, add a line stating whether the run
  will be concurrent (e.g. "Running N loaders concurrently (DuckLake)." vs
  "Running sequentially (DuckDB backend).") so the behavior is visible.
- Update the `RunScreen` docstring — it currently claims "Jobs run
  sequentially"; make it backend-dependent.

### 4. CLI batch command — new `loaders/load_all.py` (+ registry)
A top-level utility (no `WATERMARKS`, so it registers at the top level like
`state`/`drop-db`, not under `load`): `buehrle load-all`.

- `register(subparsers)`: positional `loaders` (zero or more command names;
  empty = every `data_loaders()` loader), `--full-refresh` (full-rebuild mode;
  default is the grid's incremental resolution), `--max-concurrency N`
  (overrides the env default), `--sequential` (force one-at-a-time).
- `main(parser, args)`:
  1. Resolve rows/status the same way the grid does, then build `Job`s.
     **Reuse, don't duplicate:** factor the status→rows pipeline currently in
     `app.load_rows()` (app.py:57) into a non-UI module (it does DuckDB reads
     via `open_read_connection` + `state.loader_status` + `core.build_row`;
     move it to `loaders/state.py`, which already owns those reads, so the CLI
     doesn't import Textual). The TUI then calls the moved function too.
  2. Apply the action to the selected rows (set `selection` to `INCREMENTAL` or
     `FULL`), then `core.loader_jobs(rows, today)` to get `Job`s — identical to
     the grid path.
  3. Compute `max_workers` via `worker_count(...)` (honoring `--sequential` /
     `--max-concurrency`), then `runner.run_jobs(jobs, print, max_workers=…)`.
  4. If concurrency was requested but backend is `duckdb`, print a clear notice
     that it's falling back to sequential (the gate already forces
     `max_workers=1`); exit non-zero if any job failed.
- Register in `loaders/registry.py`: import `load_all`, add to `LOADERS`. It
  lands in `utilities()` automatically (no `WATERMARKS`).

### 5. Docs
- `README.md` / `AGENTS.md`: document `buehrle load-all`, that concurrency
  requires `BUEHRLE_BACKEND=ducklake`, and `BUEHRLE_MAX_CONCURRENCY` (next to
  the existing `BUEHRLE_BACKEND` / `BUEHRLE_DB` notes).

## Why this keeps both backends cheap to maintain

The two lanes share `_run_one_job` (the hard part). They diverge only in
orchestration — a sequential loop vs a bounded pool — selected by one guard
(`worker_count`). Nothing backend-specific leaks into the runner: each loader
subprocess already inherits `BUEHRLE_BACKEND` from the environment. The DuckDB
path is unchanged and its existing test still passes.

## Files to modify / add
- `loaders/interactive/runner.py` — extract `_run_one_job`; add
  `stream_jobs_concurrent`, `run_jobs`.
- `loaders/interactive/core.py` — `resolve_max_concurrency`, `worker_count`.
- `loaders/interactive/app.py` — call `run_jobs`; concurrency notice; docstring.
- `loaders/state.py` — house the shared status→rows loader (moved from app.py).
- `loaders/load_all.py` — **new** `load-all` command.
- `loaders/registry.py` — register `load_all`.
- `README.md` / `AGENTS.md` — docs.
- Tests: `worker_count` / `resolve_max_concurrency` (core), the concurrent
  runner path (all jobs run, failures collected, labels interleave — use the
  injectable `base_argv`), and `load-all` arg→job resolution.

## Verification
1. `docker compose up -d`; confirm Postgres healthy on 5432.
2. **Concurrency (the goal):**
   `BUEHRLE_BACKEND=ducklake uv run buehrle load-all lahman chadwick-register`
   — both commit, output lines interleave with `[label]` prefixes, no lock
   error. Confirm both schemas populate via a DuckLake attach.
3. **TUI:** with `BUEHRLE_BACKEND=ducklake`, select 2+ loaders in the grid and
   press `r` — they run in parallel (interleaved output, faster wall-clock).
4. **Gate holds:** the same TUI selection on the default DuckDB backend runs
   sequentially (one `[label]` block at a time); `buehrle load-all …` on DuckDB
   prints the sequential-fallback notice.
5. **Cap:** `BUEHRLE_MAX_CONCURRENCY=2 … load-all <5 loaders>` never runs more
   than 2 at once; `--sequential` forces one.
6. **No regressions:** `task smoke -- mlb-statsapi-schedules --date 2025-04-01`
   (throwaway DuckDB) and `task check` (lint + coverage) pass.
