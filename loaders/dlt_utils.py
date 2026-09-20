import os

import dlt
import polars as pl
import pyarrow as pa
from dlt.destinations.exceptions import DatabaseUndefinedRelation

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), '../data/buehrle-raw.duckdb')

_DEFAULT_DUCKLAKE_DSN = 'postgresql://buehrle:buehrle@localhost:5432/buehrle_ducklake'
_DEFAULT_DUCKLAKE_STORAGE = os.path.join(os.path.dirname(__file__), '../data/ducklake/')


def resolve_backend() -> str:
    """Active destination backend: ``'duckdb'`` (default) or ``'ducklake'``.

    Reads ``BUEHRLE_BACKEND`` at call time so it can be set per-invocation.
    """
    return os.environ.get('BUEHRLE_BACKEND', 'duckdb')


def ducklake_catalog_dsn() -> str:
    """Postgres DSN for the DuckLake catalog.

    Override with ``BUEHRLE_DUCKLAKE_CATALOG``; defaults to the local Docker
    instance defined in ``docker-compose.yml``.
    """
    return os.environ.get('BUEHRLE_DUCKLAKE_CATALOG', _DEFAULT_DUCKLAKE_DSN)


def ducklake_storage_path() -> str:
    """Directory where DuckLake writes Parquet data files.

    Override with ``BUEHRLE_DUCKLAKE_STORAGE``; defaults to ``data/ducklake/``
    under the repo root.
    """
    return os.environ.get('BUEHRLE_DUCKLAKE_STORAGE', _DEFAULT_DUCKLAKE_STORAGE)


def resolve_db_path() -> str:
    """The DuckDB file every loader writes to.

    Honors the ``BUEHRLE_DB`` env var so smoke tests (and any tooling) can
    redirect loads to a throwaway database without touching the real one;
    falls back to the checked-in ``data/buehrle-raw.duckdb``. Read at call time
    so the override can be set per-invocation.
    """
    return os.environ.get('BUEHRLE_DB') or DEFAULT_DB_PATH


def make_pipeline(name: str):
    if resolve_backend() == 'ducklake':
        from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials
        credentials = DuckLakeCredentials(
            catalog=ducklake_catalog_dsn(),
            storage=ducklake_storage_path(),
        )
        destination = dlt.destinations.ducklake(credentials=credentials)
    else:
        destination = dlt.destinations.duckdb(resolve_db_path())
    return dlt.pipeline(
        pipeline_name=name,
        destination=destination,
        dataset_name=name,
    )


def open_read_connection(db_path: str | None = None):
    """Open a read-only connection to whichever backend is active.

    - ``duckdb``: connects directly to the ``.duckdb`` file, read-only.
      ``db_path`` overrides ``resolve_db_path()`` so callers that accept a
      ``--db`` flag (e.g. ``state``, the TUI) can honour it.
    - ``ducklake``: opens an in-memory DuckDB session, installs the required
      extensions, attaches the DuckLake catalog read-only, and sets it as the
      active database so callers can use the same ``information_schema`` and
      ``"schema"."table"`` queries used everywhere. ``db_path`` is ignored.

    The caller is responsible for closing the returned connection.
    """
    import duckdb

    if resolve_backend() == 'ducklake':
        con = duckdb.connect()
        con.execute('INSTALL ducklake; LOAD ducklake;')
        con.execute('INSTALL postgres; LOAD postgres;')
        dsn = ducklake_catalog_dsn()
        storage = ducklake_storage_path()
        con.execute(
            f"ATTACH 'ducklake:postgres:{dsn}' AS ducklake "
            f"(DATA_PATH '{storage}', READ_ONLY)"
        )
        con.execute('USE ducklake;')
        return con
    else:
        return duckdb.connect(db_path or resolve_db_path(), read_only=True)


def handle_full_refresh(pipeline) -> None:
    with pipeline.destination_client() as client:
        try:
            client.drop_storage()
        except DatabaseUndefinedRelation:
            pass
    pipeline.drop()


def to_arrow(df: pl.DataFrame, primary_keys: set[str]) -> pa.Table:
    table = df.to_arrow()
    schema = pa.schema([
        (f.with_type(pa.utf8()) if f.type == pa.large_utf8() else f)
            .with_nullable(f.name not in primary_keys)
        for f in table.schema
    ])
    return table.cast(schema)
