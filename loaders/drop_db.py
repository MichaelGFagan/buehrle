"""Delete the raw DuckDB file from disk.

This is the blunt reset: it removes the entire database file that every loader
writes to, not just one loader's schema (that's what a loader's
``--full-refresh`` does). Use it to start completely clean.

Registered as the ``drop-db`` subcommand. It is destructive, so it requires an
explicit ``--yes`` confirmation (or an interactive y/N prompt).
"""

from __future__ import annotations

from pathlib import Path

from loaders.dlt_utils import DB_PATH

DEFAULT_DB = Path(DB_PATH).resolve()


def register(subparsers):
    parser = subparsers.add_parser(
        'drop-db',
        help='Delete the raw DuckDB file from disk (destructive).',
    )
    parser.add_argument('--db', type=Path, default=DEFAULT_DB,
                        help=f'Path to DuckDB file (default: {DEFAULT_DB})')
    parser.add_argument('--yes', action='store_true',
                        help='Skip the confirmation prompt.')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args) -> None:
    db = Path(args.db).resolve()

    if not db.exists():
        print(f'Nothing to drop: {db} does not exist.')
        return

    if not args.yes:
        reply = input(f'Delete {db}? This cannot be undone. [y/N] ').strip().lower()
        if reply not in ('y', 'yes'):
            print('Aborted.')
            return

    db.unlink()
    # DuckDB may also leave a write-ahead-log file alongside the database.
    wal = db.with_name(db.name + '.wal')
    if wal.exists():
        wal.unlink()

    print(f'Dropped {db}.')
