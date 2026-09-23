"""Report the current state of each loader by inspecting the DuckDB destination.

This is the read-only **status view**: one row per loader (the canonical set,
driven by the loader registry — not by whatever schemas happen to exist) showing
its last load and its oldest table watermark. The interactive status grid reads
the same per-loader status via :func:`loader_status`.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

DEFAULT_DB = Path(
    os.environ.get('BUEHRLE_DB')
    or Path(__file__).resolve().parent.parent / 'data' / 'buehrle-raw.duckdb'
)


def schema_exists(con: duckdb.DuckDBPyConnection, schema: str) -> bool:
    return con.execute(
        'SELECT 1 FROM information_schema.schemata WHERE schema_name = ?',
        [schema],
    ).fetchone() is not None


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return con.execute(
        'SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?',
        [schema, table],
    ).fetchone() is not None


def data_tables(con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
    rows = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name NOT LIKE '\\_dlt%' ESCAPE '\\'
        ORDER BY table_name
    """, [schema]).fetchall()
    return [t for (t,) in rows]


def has_load_id(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return con.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ? AND column_name = '_dlt_load_id'
    """, [schema, table]).fetchone() is not None


def schema_last_load(con: duckdb.DuckDBPyConnection, schema: str) -> tuple[datetime | None, int]:
    """When the loader last wrote, and how many successful loads it has run.

    Returns ``(None, 0)`` for a never-loaded loader (no ``_dlt_loads`` ledger).
    """
    if not table_exists(con, schema, '_dlt_loads'):
        return (None, 0)
    last_load, load_count = con.execute(f"""
        SELECT MAX(inserted_at), COUNT(*)
        FROM "{schema}"._dlt_loads
        WHERE status = 0
    """).fetchone()
    return (last_load, load_count or 0)


def table_watermark(con: duckdb.DuckDBPyConnection, schema: str, table: str, expr: str) -> str | None:
    """High-water mark of one table: ``MAX(expr)`` as a string.

    ``expr`` is the loader-declared SQL expression for the table's time
    dimension (usually a bare column, e.g. ``season``; occasionally an
    expression, e.g. ``substr(game_id, 4, 4)``). Returns ``None`` if the table
    is absent or empty.
    """
    if not table_exists(con, schema, table):
        return None
    value = con.execute(f'SELECT MAX({expr}) FROM "{schema}"."{table}"').fetchone()[0]
    return None if value is None else str(value)


def loader_watermarks(con: duckdb.DuckDBPyConnection, schema: str,
                      watermarks: dict[str, str]) -> dict[str, str | None]:
    """Per-table high-water marks for a loader's declared ``WATERMARKS``."""
    return {
        table: table_watermark(con, schema, table, expr)
        for table, expr in watermarks.items()
    }


def oldest_watermark(values: Iterable[str | None]) -> str | None:
    """The laggard watermark across a loader's tables — what smart-incremental
    loads forward from.

    Returns ``None`` when the loader declares no watermarks (single-shot
    loaders) **or** any watermarked table is absent/empty. A missing table
    means the loader isn't fully current, so the planner falls back to a full
    backfill rather than skipping that table's early history.

    Watermarks within a loader share one fixed-width, lexicographically-ordered
    time dimension (seasons like ``'2024'`` or dates like ``'2024-04-28'`` /
    ``'20240428'``), so ``min`` over the strings is chronological.
    """
    values = list(values)
    if not values or any(value is None for value in values):
        return None
    return min(values)


@dataclass
class LoaderStatus:
    """Everything the status view / grid needs to describe one loader."""
    schema: str                       # PIPELINE_NAME — destination schema and row identity
    table_count: int                  # data tables currently in the schema (0 if never loaded)
    last_load: datetime | None
    load_count: int
    watermarks: dict[str, str | None]  # per declared table, None if absent/empty
    oldest: str | None                 # laggard watermark, or None => full-history / never-loaded
    full_refresh_only: bool            # True when the loader declares no watermarks


def loader_status(con: duckdb.DuckDBPyConnection, module) -> LoaderStatus:
    """Read one loader's status from the destination, keyed by its
    ``PIPELINE_NAME`` (schema) and ``WATERMARKS``.
    """
    schema = module.PIPELINE_NAME
    watermarks = loader_watermarks(con, schema, module.WATERMARKS)
    last_load, load_count = schema_last_load(con, schema)
    return LoaderStatus(
        schema=schema,
        table_count=len(data_tables(con, schema)),
        last_load=last_load,
        load_count=load_count,
        watermarks=watermarks,
        oldest=oldest_watermark(watermarks.values()),
        full_refresh_only=not module.WATERMARKS,
    )


def _never_loaded_status(module) -> LoaderStatus:
    """Synthesise a never-loaded status row for a loader with no destination
    schema yet (used on the duckdb backend when the DB file is absent)."""
    return LoaderStatus(
        schema=module.PIPELINE_NAME,
        table_count=0,
        last_load=None,
        load_count=0,
        watermarks={table: None for table in module.WATERMARKS},
        oldest=None,
        full_refresh_only=not module.WATERMARKS,
    )


def load_rows(db):
    """Read every registered loader's status into grid rows.

    Shared by the interactive grid and the ``loads`` CLI so neither
    duplicates the status->rows pipeline (and the CLI needn't import Textual).

    For the duckdb backend, a missing DB file means nothing has been loaded
    yet, so every row is synthesised as never-loaded rather than erroring.
    For the ducklake backend, the file check is meaningless, so we always
    open the catalog and let :func:`loader_status` yield never-loaded rows for
    any schema that doesn't exist yet.
    """
    import duckdb

    from loaders.dlt_utils import open_read_connection, resolve_backend
    from loaders.interactive.core import build_row
    from loaders.registry import data_loaders

    db = Path(db)
    modules = data_loaders()
    if resolve_backend() == 'ducklake' or db.exists():
        try:
            con = open_read_connection(str(db))
        except duckdb.InvalidInputException:
            # Fresh DuckLake: the catalog doesn't exist until the first load
            # creates it, and the read path won't create it. Treat as
            # never-loaded, mirroring a missing DuckDB file.
            statuses = [_never_loaded_status(module) for module in modules]
        else:
            try:
                statuses = [loader_status(con, module) for module in modules]
            finally:
                con.close()
    else:
        statuses = [_never_loaded_status(module) for module in modules]
    return [build_row(module, status) for module, status in zip(modules, statuses)]


def table_row(con: duckdb.DuckDBPyConnection, schema: str, table: str,
              watermark: str | None) -> tuple:
    rows = con.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
    if has_load_id(con, schema, table):
        last_load, load_count = con.execute(f"""
            SELECT MAX(l.inserted_at), COUNT(DISTINCT t._dlt_load_id)
            FROM "{schema}"."{table}" t
            JOIN "{schema}"._dlt_loads l ON l.load_id = t._dlt_load_id
            WHERE l.status = 0
        """).fetchone()
    else:
        last_load, load_count = None, None
    return (schema, table, rows, last_load, load_count, watermark)


def fmt(value) -> str:
    if value is None:
        return '-'
    if hasattr(value, 'strftime'):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, int):
        return f'{value:,}'
    return str(value)


def print_table(headers: list[str], rows: list[tuple]) -> None:
    cells = [headers] + [[fmt(c) for c in r] for r in rows]
    widths = [max(len(row[i]) for row in cells) for i in range(len(headers))]
    sep = '  '
    print(sep.join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    print(sep.join('-' * widths[i] for i in range(len(headers))))
    for r in cells[1:]:
        print(sep.join(c.ljust(widths[i]) for i, c in enumerate(r)))


def register(subparsers):
    parser = subparsers.add_parser('state', help='Report current state of each loader from the DuckDB destination')
    parser.add_argument('--mode', choices=['schema', 'table'], default='schema',
                        help='Report granularity (default: schema)')
    parser.add_argument('--db', type=Path, default=DEFAULT_DB,
                        help=f'Path to DuckDB file (default: {DEFAULT_DB}); ignored when BUEHRLE_BACKEND=ducklake')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args) -> None:
    import duckdb

    from loaders.dlt_utils import open_read_connection
    from loaders.registry import data_loaders  # lazy: registry imports this module

    loaders = data_loaders()
    try:
        con = open_read_connection(str(args.db))
    except duckdb.InvalidInputException:
        # Fresh DuckLake catalog (not yet created by any load): report every
        # loader as never-loaded rather than erroring.
        if args.mode == 'schema':
            print_table(['loader', 'tables', 'last_load', 'loads', 'watermark'],
                        [(m.PIPELINE_NAME, 0, None, 0, None) for m in loaders])
        else:
            print_table(['loader', 'table', 'rows', 'last_load', 'loads', 'watermark'],
                        [(m.PIPELINE_NAME, None, 0, None, 0, None) for m in loaders])
        return
    statuses = [loader_status(con, module) for module in loaders]

    if args.mode == 'schema':
        rows = [
            (st.schema, st.table_count, st.last_load, st.load_count, st.oldest)
            for st in statuses
        ]
        print_table(['loader', 'tables', 'last_load', 'loads', 'watermark'], rows)
    else:
        rows = []
        for st in statuses:
            tables = data_tables(con, st.schema)
            if not tables:
                rows.append((st.schema, None, 0, st.last_load, st.load_count, None))
                continue
            for table in tables:
                rows.append(table_row(con, st.schema, table, st.watermarks.get(table)))
        print_table(['loader', 'table', 'rows', 'last_load', 'loads', 'watermark'], rows)
