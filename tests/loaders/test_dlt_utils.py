import polars as pl
import pyarrow as pa

from loaders.dlt_utils import to_arrow


def test_large_utf8_pk_column_becomes_utf8_non_nullable():
    df = pl.DataFrame({'player_id': ['a', 'b']})
    table = to_arrow(df, primary_keys={'player_id'})
    field = table.schema.field('player_id')
    assert field.type == pa.utf8()
    assert field.nullable is False


def test_large_utf8_non_pk_column_becomes_utf8_nullable():
    df = pl.DataFrame({'name': ['a', 'b']})
    table = to_arrow(df, primary_keys=set())
    field = table.schema.field('name')
    assert field.type == pa.utf8()
    assert field.nullable is True


def test_non_string_column_preserves_type():
    df = pl.DataFrame({'count': [1, 2, 3]})
    table = to_arrow(df, primary_keys={'count'})
    field = table.schema.field('count')
    assert field.type == pa.int64()


def test_mixed_columns():
    df = pl.DataFrame({
        'player_id': ['x', 'y'],
        'name': ['Alice', 'Bob'],
        'count': [1, 2],
    })
    table = to_arrow(df, primary_keys={'player_id'})
    assert table.schema.field('player_id').type == pa.utf8()
    assert table.schema.field('player_id').nullable is False
    assert table.schema.field('name').type == pa.utf8()
    assert table.schema.field('name').nullable is True
    assert table.schema.field('count').type == pa.int64()


def test_data_preserved():
    df = pl.DataFrame({
        'player_id': ['x', 'y', 'z'],
        'count': [1, 2, 3],
    })
    table = to_arrow(df, primary_keys={'player_id'})
    assert pl.from_arrow(table).to_dicts() == df.to_dicts()


def test_empty_dataframe():
    df = pl.DataFrame({'player_id': pl.Series([], dtype=pl.Utf8)})
    table = to_arrow(df, primary_keys={'player_id'})
    field = table.schema.field('player_id')
    assert table.num_rows == 0
    assert field.type == pa.utf8()
    assert field.nullable is False
