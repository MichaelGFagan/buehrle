"""Batch-run loaders from the command line: the non-interactive twin of the
status grid's "press r to run" flow.

Registered as the top-level ``loads`` subcommand (it lands no data of its
own, so it sits beside ``state`` / ``drop-db`` rather than under ``load``).
It resolves the same watermark-driven plan the grid would, builds the same
:class:`~loaders.interactive.core.Job` list, and runs them through the shared
runner - concurrently on the DuckLake backend, sequentially on DuckDB.

Examples:
    buehrle loads                          # every loader, incremental
    buehrle loads lahman chadwick-register
    buehrle loads --full-refresh           # clean rebuild of every loader
    buehrle loads --sequential             # force one-at-a-time
"""

from __future__ import annotations

import datetime
import threading
from pathlib import Path

from loaders.interactive.core import (
    FULL,
    INCREMENTAL,
    loader_jobs,
    resolve_max_concurrency,
    selected_commands,
    worker_count,
)
from loaders.state import DEFAULT_DB, load_rows

DEFAULT_LOG_DIR = Path('logs')


def register(subparsers):
    parser = subparsers.add_parser(
        'loads',
        help='Run many loaders at once (concurrent on the DuckLake backend).',
    )
    parser.add_argument(
        'loaders', nargs='*', metavar='<loader>',
        help='Loader command names to run (default: every data loader).',
    )
    parser.add_argument(
        '--full-refresh', action='store_true',
        help='Clean rebuild each loader (--full-refresh --full-history) '
             'instead of the default incremental resolution.',
    )
    parser.add_argument(
        '--max-concurrency', type=int, default=None, metavar='N',
        help='Cap on loaders running at once (default: BUEHRLE_MAX_CONCURRENCY '
             'or 4). Ignored on the DuckDB backend, which runs sequentially.',
    )
    parser.add_argument(
        '--sequential', action='store_true',
        help='Force one-at-a-time even on the DuckLake backend.',
    )
    parser.add_argument(
        '--db', type=Path, default=DEFAULT_DB,
        help=f'Path to DuckDB file (default: {DEFAULT_DB}); '
             'ignored when BUEHRLE_BACKEND=ducklake.',
    )
    parser.add_argument(
        '--no-log-file', action='store_true',
        help=f'Do not tee output to a log file (logs go to {DEFAULT_LOG_DIR}/ '
             'by default).',
    )
    parser.add_argument(
        '--log-file', type=Path, default=None, metavar='PATH',
        help=f'Write the run log here instead of the default '
             f'{DEFAULT_LOG_DIR}/loads-<timestamp>.log.',
    )
    parser.set_defaults(func=lambda args: main(parser, args))


def _select_rows(rows, requested: list[str], parser):
    """Set each requested row's selection; error on unknown names.

    With no names, every loader is selected. ``--full-refresh`` maps to FULL;
    otherwise incremental-capable loaders get INCREMENTAL and full-refresh-only
    loaders get FULL (their only mode).
    """
    by_command = {row.command: row for row in rows}
    if requested:
        unknown = [name for name in requested if name not in by_command]
        if unknown:
            parser.error(f'unknown loader(s): {", ".join(unknown)}')
        chosen = [by_command[name] for name in requested]
    else:
        chosen = rows
    return chosen


def _open_log(args):
    """The log file to tee run output into, or ``None`` under ``--no-log-file``.

    Returns ``(path, file)``; the default path is a timestamped file under
    ``logs/`` so parallel runs never clobber each other.
    """
    if args.no_log_file:
        return None, None
    if args.log_file is not None:
        path = Path(args.log_file)
    else:
        stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        path = DEFAULT_LOG_DIR / f'loads-{stamp}.log'
    path.parent.mkdir(parents=True, exist_ok=True)
    return path, path.open('w')


def _make_writer(fh):
    """A thread-safe writer that prints each line and tees it to ``fh`` (if any).

    The lock keeps a line's stdout and file writes together under the concurrent
    runner, where several loader threads call this at once.
    """
    lock = threading.Lock()

    def write(line: str) -> None:
        with lock:
            print(line)
            if fh is not None:
                fh.write(line + '\n')
                fh.flush()
    return write


def main(parser, args) -> None:
    from loaders.dlt_utils import resolve_backend
    from loaders.interactive.runner import run_jobs

    rows = load_rows(args.db)
    chosen = _select_rows(rows, args.loaders, parser)
    for row in chosen:
        if args.full_refresh or not row.can_incremental():
            row.selection = FULL
        else:
            row.selection = INCREMENTAL

    today = datetime.date.today()
    jobs = loader_jobs(chosen, today)
    if not jobs:
        print('Nothing to run.')
        return

    log_path, fh = _open_log(args)
    write = _make_writer(fh)
    if log_path is not None:
        write(f'Logging to {log_path}')

    write('Planned loads:')
    for command, flags in selected_commands(chosen, today):
        write(f'  buehrle load {command} {" ".join(flags)}')

    backend = resolve_backend()
    cap = args.max_concurrency if args.max_concurrency is not None else resolve_max_concurrency()
    cap = max(1, cap)
    max_workers = 1 if args.sequential else worker_count(backend, len(jobs), cap)

    if backend == 'duckdb' and not args.sequential and len(jobs) > 1:
        write('Note: DuckDB is single-writer; running sequentially. '
              'Set BUEHRLE_BACKEND=ducklake for concurrent loads.')
    elif max_workers > 1:
        write(f'Running up to {max_workers} loaders concurrently (DuckLake).')

    try:
        failures = run_jobs(jobs, write, max_workers=max_workers)
    finally:
        if fh is not None:
            fh.close()

    if log_path is not None:
        print(f'Full log written to {log_path}')
    if failures:
        raise SystemExit(1)
