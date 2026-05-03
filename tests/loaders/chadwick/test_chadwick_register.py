import runpy
import sys

import dlt
import duckdb
import pyarrow as pa
import responses

from loaders.chadwick import chadwick_register


def _mock(character: str, status: int = 200, body: str = 'key_uuid,first_name\nabc,John'):
    responses.add(
        responses.GET,
        f'{chadwick_register.BASE_URL}/people-{character}.csv',
        body=body,
        status=status,
    )


@responses.activate
def test_people_404_on_first_character_yields_nothing():
    _mock('0', status=404)
    assert list(chadwick_register.people()) == []


@responses.activate
def test_people_stops_at_first_404_after_yielding():
    _mock('0')
    _mock('1', status=404)

    result = list(chadwick_register.people())
    assert len(result) == 1
    assert isinstance(result[0], pa.Table)


@responses.activate
def test_people_yields_all_36_when_all_succeed():
    for c in chadwick_register.CHARACTERS:
        _mock(c)

    result = list(chadwick_register.people())
    assert len(result) == 36


def test_people_uses_timeout(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 404
        content = b''
        def raise_for_status(self):
            pass

    def fake_get(url, **kwargs):
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr('loaders.chadwick.chadwick_register.requests.get', fake_get)
    list(chadwick_register.people())
    assert captured['timeout'] == 30


def _build_pipeline(tmp_path, name='chadwick_register_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


@responses.activate
def test_pipeline_loads_data_into_duckdb(tmp_path):
    _mock('0', body='key_uuid,name_first\n0001,Alice\n0002,Bob')
    _mock('1', body='key_uuid,name_first\n1001,Carol')
    _mock('2', status=404)

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(chadwick_register.chadwick_register())

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT key_uuid, name_first FROM chadwick_register_test.people ORDER BY key_uuid'
    ).fetchall()
    assert rows == [('0001', 'Alice'), ('0002', 'Bob'), ('1001', 'Carol')]


@responses.activate
def test_pipeline_pk_column_is_non_nullable_in_duckdb(tmp_path):
    _mock('0', body='key_uuid,name_first\n0001,Alice')
    _mock('1', status=404)

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(chadwick_register.chadwick_register())

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    schema = con.execute(
        "SELECT column_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'chadwick_register_test' "
        "AND table_name = 'people' AND column_name = 'key_uuid'"
    ).fetchall()
    assert schema == [('key_uuid', 'NO')]


@responses.activate
def test_main_executes(monkeypatch, fake_make_pipeline):
    _mock('0', body='key_uuid,name\n0001,Alice')
    _mock('1', status=404)

    monkeypatch.setattr('loaders.dlt_utils.make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['chadwick_register', '--full-refresh'])
    runpy.run_module('loaders.chadwick.chadwick_register', run_name='__main__')
