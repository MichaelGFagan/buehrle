"""Report current state of each loader by inspecting the DuckDB destination."""

from __future__ import annotations

from pathlib import Path

import duckdb

DEFAULT_DB = Path(__file__).resolve().parent.parent / 'data' / 'buehrle.duckdb'
SYSTEM_SCHEMAS = {'information_schema', 'main', 'pg_catalog'}


def loader_schemas(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY schema_name
    """).fetchall()
    return [
        s for (s,) in rows
        if s not in SYSTEM_SCHEMAS and not s.endswith('_staging')
    ]


def data_tables(con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
    rows = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name NOT LIKE '\\_dlt%' ESCAPE '\\'
        ORDER BY table_name
    """, [schema]).fetchall()
    return [t for (t,) in rows]


def schema_row(con: duckdb.DuckDBPyConnection, schema: str) -> tuple:
    tables = data_tables(con, schema)
    last_load, load_count = con.execute(f"""
        SELECT MAX(inserted_at), COUNT(*)
        FROM "{schema}"._dlt_loads
        WHERE status = 0
    """).fetchone()
    return (schema, len(tables), last_load, load_count)


def has_load_id(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return con.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ? AND column_name = '_dlt_load_id'
    """, [schema, table]).fetchone() is not None


def table_row(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> tuple:
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
    return (schema, table, rows, last_load, load_count)


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
                        help=f'Path to DuckDB file (default: {DEFAULT_DB})')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args) -> None:
    con = duckdb.connect(str(args.db), read_only=True)
    schemas = loader_schemas(con)

    if args.mode == 'schema':
        rows = [schema_row(con, s) for s in schemas]
        print_table(['loader', 'tables', 'last_load', 'loads'], rows)
    else:
        rows = [
            table_row(con, s, t)
            for s in schemas
            for t in data_tables(con, s)
        ]
        print_table(['loader', 'table', 'rows', 'last_load', 'loads'], rows)
