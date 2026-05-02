import os
import dlt
import polars as pl
import pyarrow as pa

from dlt.destinations.exceptions import DatabaseUndefinedRelation

DB_PATH = os.path.join(os.path.dirname(__file__), '../data/buehrle.duckdb')


def make_pipeline(name: str):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(DB_PATH),
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
        f.with_type(pa.utf8()).with_nullable(f.name not in primary_keys)
        if f.type == pa.large_utf8()
        else f
        for f in table.schema
    ])
    return table.cast(schema)
