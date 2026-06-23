import logging
import os
import sys

import duckdb
import pyarrow as pa
import pytest
from dlt.extract.exceptions import ResourceExtractionError

import loaders.__main__ as loaders_main
from loaders.lahman import lahman


BATTING_CSV = (
    'playerID,yearID,stint,teamID,lgID,G,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP\n'
    'doejo01,2024,1,NYY,AL,15,50,10,15,3,1,2,8,1,0,5,12,0,1,0,0,1\n'
    'doeja02,2024,1,BOS,AL,20,70,12,20,4,0,3,10,2,1,6,15,0,0,0,1,0\n'
)

PEOPLE_CSV = (
    'playerID,birthYear,nameFirst,nameLast\n'
    'doejo01,1990,John,Doe\n'
    'doeja02,1992,Jane,Doe\n'
)


def _write(data_dir, filename, body):
    path = os.path.join(data_dir, filename)
    with open(path, 'w') as f:
        f.write(body)
    return path


def _stub_csv(num_fields):
    header = ','.join(f'c{i}' for i in range(num_fields))
    row = ','.join(str(i) for i in range(num_fields))
    return f'{header}\n{row}\n'


def _populate_all_tables(data_dir):
    for _, csv_filename, columns in lahman.TABLES:
        width = len(columns) if columns is not None else 2
        _write(str(data_dir), csv_filename, _stub_csv(width))


def test_check_for_unmapped_csvs_warns_for_unknown_file(tmp_path, caplog):
    _write(str(tmp_path), 'People.csv', PEOPLE_CSV)
    _write(str(tmp_path), 'NewSabrTable.csv', 'a,b\n1,2\n')

    with caplog.at_level(logging.WARNING):
        lahman._check_for_unmapped_csvs(str(tmp_path))

    assert any('NewSabrTable.csv' in r.message for r in caplog.records)


def test_check_for_unmapped_csvs_silent_when_all_mapped(tmp_path, caplog):
    _write(str(tmp_path), 'People.csv', PEOPLE_CSV)

    with caplog.at_level(logging.WARNING):
        lahman._check_for_unmapped_csvs(str(tmp_path))

    assert caplog.records == []


def test_load_csv_with_column_override_lowercases_and_renames(tmp_path):
    path = _write(str(tmp_path), 'Batting.csv', BATTING_CSV)
    table = lahman._load_csv(path, lahman.BATTING_COLUMNS)

    assert isinstance(table, pa.Table)
    assert '_2b' in table.column_names
    assert '_3b' in table.column_names
    assert 'playerid' in table.column_names
    assert all(c == c.lower() for c in table.column_names)
    assert table.num_rows == 2
    for field in table.schema:
        assert field.type != pa.large_utf8()


def test_load_csv_without_override_uses_header(tmp_path):
    path = _write(str(tmp_path), 'People.csv', PEOPLE_CSV)
    table = lahman._load_csv(path, None)

    assert table.column_names == ['playerid', 'birthyear', 'namefirst', 'namelast']
    assert table.num_rows == 2


def test_resource_raises_when_csv_missing(tmp_path):
    source = lahman.lahman(data_dir=str(tmp_path))
    people_resource = source.resources['people']

    with pytest.raises(ResourceExtractionError, match='People.csv'):
        list(people_resource)


def test_pipeline_loads_csvs_into_duckdb(tmp_path, fake_make_pipeline):
    data_dir = tmp_path / 'lahman_data'
    data_dir.mkdir()
    _write(str(data_dir), 'People.csv', PEOPLE_CSV)
    _write(str(data_dir), 'Batting.csv', BATTING_CSV)

    pipeline = fake_make_pipeline('lahman')
    source = lahman.lahman(data_dir=str(data_dir)).with_resources('people', 'batting')
    pipeline.run(source)

    db_path = str(tmp_path / 'test.duckdb')
    con = duckdb.connect(db_path)

    people_rows = con.execute(
        'SELECT playerid, namefirst, namelast FROM lahman.people ORDER BY playerid'
    ).fetchall()
    assert people_rows == [('doeja02', 'Jane', 'Doe'), ('doejo01', 'John', 'Doe')]

    batting_cols = {
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'lahman' AND table_name = 'batting'"
        ).fetchall()
    }
    assert {'_2b', '_3b', 'playerid', 'teamid'}.issubset(batting_cols)

    batting_rows = con.execute(
        'SELECT playerid, _2b, _3b FROM lahman.batting ORDER BY playerid'
    ).fetchall()
    assert batting_rows == [('doeja02', '4', '0'), ('doejo01', '3', '1')]


def test_pipeline_replaces_on_rerun(tmp_path, fake_make_pipeline):
    data_dir = tmp_path / 'lahman_data'
    data_dir.mkdir()
    _write(str(data_dir), 'People.csv', PEOPLE_CSV)

    pipeline = fake_make_pipeline('lahman')
    pipeline.run(lahman.lahman(data_dir=str(data_dir)).with_resources('people'))

    _write(str(data_dir), 'People.csv', 'playerID,birthYear,nameFirst,nameLast\nnewpl01,2000,New,Player\n')
    pipeline.run(lahman.lahman(data_dir=str(data_dir)).with_resources('people'))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute('SELECT playerid FROM lahman.people').fetchall()
    assert rows == [('newpl01',)]


def test_main_executes(tmp_path, monkeypatch, fake_make_pipeline):
    data_dir = tmp_path / 'lahman_data'
    data_dir.mkdir()
    _populate_all_tables(data_dir)

    monkeypatch.setattr('loaders.dlt_utils.make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'buehrle', 'load', 'lahman', '--data-dir', str(data_dir), '--full-refresh',
    ])
    loaders_main.main()


def test_main_warns_on_unmapped_csv(tmp_path, monkeypatch, fake_make_pipeline, caplog):
    data_dir = tmp_path / 'lahman_data'
    data_dir.mkdir()
    _populate_all_tables(data_dir)
    _write(str(data_dir), 'NewSabrTable.csv', 'a,b\n1,2\n')

    monkeypatch.setattr('loaders.dlt_utils.make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['buehrle', 'load', 'lahman', '--data-dir', str(data_dir)])

    with caplog.at_level(logging.WARNING):
        loaders_main.main()

    assert any('NewSabrTable.csv' in r.message for r in caplog.records)
