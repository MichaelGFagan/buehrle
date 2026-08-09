import os

import dlt
import polars as pl
import pyarrow as pa
from dlt.destinations.exceptions import DatabaseUndefinedRelation

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), '../data/buehrle-raw.duckdb')


def resolve_db_path() -> str:
    """The DuckDB file every loader writes to.

    Honors the ``BUEHRLE_DB`` env var so smoke tests (and any tooling) can
    redirect loads to a throwaway database without touching the real one;
    falls back to the checked-in ``data/buehrle-raw.duckdb``. Read at call time
    so the override can be set per-invocation.
    """
    return os.environ.get('BUEHRLE_DB') or DEFAULT_DB_PATH


def make_pipeline(name: str):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(resolve_db_path()),
        dataset_name=name,
    )


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
