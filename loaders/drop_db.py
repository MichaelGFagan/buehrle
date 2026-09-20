"""Reset one or both backends by destroying their stored data.

- **duckdb**: deletes the ``.duckdb`` file (and its ``.wal`` sidecar).
- **ducklake**: drops every loader schema from the Postgres catalog and clears
  the Parquet data directory. For a full wipe including the Postgres volume,
  follow up with ``docker compose down -v``.
- **both**: runs duckdb then ducklake teardown in sequence.

``--target`` defaults to the active ``BUEHRLE_BACKEND``, so the common case
requires no extra flags.

Registered as the ``drop-db`` subcommand. Requires ``--yes`` or an interactive
confirmation because it is destructive and irreversible.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from loaders.dlt_utils import (
    ducklake_catalog_dsn,
    ducklake_storage_path,
    resolve_backend,
    resolve_db_path,
)

DEFAULT_DB = Path(resolve_db_path()).resolve()


def _drop_duckdb(db: Path) -> None:
    if not db.exists():
        print(f'Nothing to drop: {db} does not exist.')
        return
    db.unlink()
    wal = db.with_name(db.name + '.wal')
    if wal.exists():
        wal.unlink()
    print(f'Dropped {db}.')


def _drop_ducklake() -> None:
    import duckdb

    dsn = ducklake_catalog_dsn()
    storage = Path(ducklake_storage_path())

    con = duckdb.connect()
    con.execute('INSTALL ducklake; LOAD ducklake;')
    con.execute('INSTALL postgres; LOAD postgres;')
    con.execute(
        f"ATTACH 'ducklake:postgres:{dsn}' AS ducklake "
        f"(DATA_PATH '{storage}')"
    )
    con.execute('USE ducklake;')

    schemas = [
        row[0]
        for row in con.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('information_schema', 'main')"
        ).fetchall()
    ]
    for schema in schemas:
        con.execute(f'DROP SCHEMA "{schema}" CASCADE')
        print(f'Dropped schema {schema}.')

    con.close()

    if storage.exists():
        shutil.rmtree(storage)
        print(f'Cleared {storage}.')
    else:
        print(f'Storage directory {storage} did not exist.')


def register(subparsers):
    parser = subparsers.add_parser(
        'drop-db',
        help='Destroy one or both backends (destructive).',
    )
    parser.add_argument(
        '--target',
        choices=['duckdb', 'ducklake', 'both'],
        default=resolve_backend(),
        help=(
            'Which backend to reset (default: active BUEHRLE_BACKEND). '
            'For a full DuckLake wipe including the Postgres volume, also run '
            '``docker compose down -v``.'
        ),
    )
    parser.add_argument('--db', type=Path, default=DEFAULT_DB,
                        help=f'Path to DuckDB file (default: {DEFAULT_DB})')
    parser.add_argument('--yes', action='store_true',
                        help='Skip the confirmation prompt.')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args) -> None:
    target = args.target

    if target == 'duckdb':
        what = str(Path(args.db).resolve())
    elif target == 'ducklake':
        what = f'DuckLake catalog at {ducklake_catalog_dsn()} and storage at {ducklake_storage_path()}'
    else:
        what = (
            f'{Path(args.db).resolve()} AND '
            f'DuckLake catalog at {ducklake_catalog_dsn()} and storage at {ducklake_storage_path()}'
        )

    if not args.yes:
        reply = input(f'Drop {what}? This cannot be undone. [y/N] ').strip().lower()
        if reply not in ('y', 'yes'):
            print('Aborted.')
            return

    if target in ('duckdb', 'both'):
        _drop_duckdb(Path(args.db).resolve())
    if target in ('ducklake', 'both'):
        _drop_ducklake()
