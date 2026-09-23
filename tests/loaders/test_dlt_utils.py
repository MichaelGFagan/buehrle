import polars as pl
import pyarrow as pa

from loaders.dlt_utils import DEFAULT_DB_PATH, ducklake_storage_path, resolve_db_path, to_arrow


def test_resolve_db_path_defaults_without_env(monkeypatch):
    monkeypatch.delenv('BUEHRLE_DB', raising=False)
    assert resolve_db_path() == DEFAULT_DB_PATH


def test_resolve_db_path_honors_env_override(monkeypatch):
    monkeypatch.setenv('BUEHRLE_DB', '/tmp/smoke.duckdb')
    assert resolve_db_path() == '/tmp/smoke.duckdb'


def test_ducklake_storage_path_normalizes_local_path(monkeypatch):
    monkeypatch.setenv('BUEHRLE_DUCKLAKE_STORAGE', '/tmp/foo/../ducklake')
    assert ducklake_storage_path() == '/tmp/ducklake/'


def test_ducklake_storage_path_passes_through_remote_uri(monkeypatch):
    monkeypatch.setenv('BUEHRLE_DUCKLAKE_STORAGE', 's3://bucket/ducklake')
    assert ducklake_storage_path() == 's3://bucket/ducklake/'


def test_ducklake_storage_path_remote_uri_keeps_single_trailing_slash(monkeypatch):
    monkeypatch.setenv('BUEHRLE_DUCKLAKE_STORAGE', 's3://bucket/ducklake/')
    assert ducklake_storage_path() == 's3://bucket/ducklake/'


def test_make_pipeline_targets_env_override(monkeypatch, tmp_path):
    from loaders.dlt_utils import make_pipeline
    target = tmp_path / 'smoke.duckdb'
    monkeypatch.setenv('BUEHRLE_DB', str(target))
    pipeline = make_pipeline('env_override_test')
    assert str(target) in pipeline.destination.config_params['credentials']


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
