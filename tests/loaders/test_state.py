"""Tests for the loader status view (loaders/state.py).

Builds a small DuckDB by hand (rather than via dlt) so each reader path —
present / empty / absent tables, never-loaded loaders, expression watermarks,
and the full-refresh-only case — can be exercised directly.
"""

from types import SimpleNamespace

import duckdb
import pytest

from loaders import state


def _module(pipeline_name, watermarks):
    """A stand-in loader module: just the attributes state.py reads."""
    return SimpleNamespace(PIPELINE_NAME=pipeline_name, WATERMARKS=watermarks)


@pytest.fixture
def db(tmp_path):
    """A DuckDB with four loaders covering the interesting shapes."""
    path = tmp_path / 'state_test.duckdb'
    con = duckdb.connect(str(path))

    # good: two watermarked tables (pit lags bat), one non-watermarked table.
    con.execute('CREATE SCHEMA good')
    con.execute('CREATE TABLE good.bat (season VARCHAR, _dlt_load_id VARCHAR)')
    con.execute("INSERT INTO good.bat VALUES ('2023', 'L1'), ('2024', 'L1')")
    con.execute('CREATE TABLE good.pit (season VARCHAR, _dlt_load_id VARCHAR)')
    con.execute("INSERT INTO good.pit VALUES ('2022', 'L1')")
    con.execute('CREATE TABLE good.meta (note VARCHAR)')  # no _dlt_load_id
    con.execute("INSERT INTO good.meta VALUES ('x')")
    con.execute('CREATE TABLE good."_dlt_loads" (load_id VARCHAR, status BIGINT, inserted_at TIMESTAMP)')
    con.execute("INSERT INTO good._dlt_loads VALUES ('L1', 0, TIMESTAMP '2026-01-01 10:00:00')")

    # events: expression watermark over game_id, single table.
    con.execute('CREATE SCHEMA events')
    con.execute('CREATE TABLE events.plays (game_id VARCHAR, _dlt_load_id VARCHAR)')
    con.execute("INSERT INTO events.plays VALUES ('ANA201804020', 'L1'), ('NYA202005010', 'L1')")
    con.execute('CREATE TABLE events."_dlt_loads" (load_id VARCHAR, status BIGINT, inserted_at TIMESTAMP)')
    con.execute("INSERT INTO events._dlt_loads VALUES ('L1', 0, TIMESTAMP '2026-02-02 12:00:00')")

    # single: full-refresh-only loader (no watermarks), one loaded table.
    con.execute('CREATE SCHEMA single')
    con.execute('CREATE TABLE single.people (id VARCHAR, _dlt_load_id VARCHAR)')
    con.execute("INSERT INTO single.people VALUES ('a', 'L1')")
    con.execute('CREATE TABLE single."_dlt_loads" (load_id VARCHAR, status BIGINT, inserted_at TIMESTAMP)')
    con.execute("INSERT INTO single._dlt_loads VALUES ('L1', 0, TIMESTAMP '2026-03-03 09:00:00')")

    con.close()
    return path


@pytest.fixture
def con(db):
    return duckdb.connect(str(db), read_only=True)


# --- pure helper -----------------------------------------------------------

@pytest.mark.parametrize('values, expected', [
    ([], None),                              # no watermarks declared
    (['2024', '2022', '2023'], '2022'),      # laggard wins
    (['2024', None], None),                  # any absent/empty => full-history
    ([None], None),
    (['2024-04-28', '2023-09-01'], '2023-09-01'),
    (['2020'], '2020'),
])
def test_oldest_watermark(values, expected):
    assert state.oldest_watermark(values) == expected


# --- DB readers ------------------------------------------------------------

def test_existence_helpers(con):
    assert state.schema_exists(con, 'good') is True
    assert state.schema_exists(con, 'ghost') is False
    assert state.table_exists(con, 'good', 'bat') is True
    assert state.table_exists(con, 'good', 'nope') is False


def test_data_tables_excludes_dlt(con):
    assert state.data_tables(con, 'good') == ['bat', 'meta', 'pit']
    assert state.data_tables(con, 'ghost') == []


def test_table_watermark_present_absent_and_expression(con):
    assert state.table_watermark(con, 'good', 'bat', 'season') == '2024'
    assert state.table_watermark(con, 'good', 'pit', 'season') == '2022'
    assert state.table_watermark(con, 'good', 'missing', 'season') is None
    # expression watermark: season is chars 4-7 of game_id
    assert state.table_watermark(con, 'events', 'plays', 'substr(game_id, 4, 4)') == '2020'


def test_table_watermark_empty_table_is_none(tmp_path):
    path = tmp_path / 'empty.duckdb'
    rw = duckdb.connect(str(path))
    rw.execute('CREATE SCHEMA s')
    rw.execute('CREATE TABLE s.empty (season VARCHAR)')  # no rows
    rw.close()
    con = duckdb.connect(str(path), read_only=True)
    assert state.table_watermark(con, 's', 'empty', 'season') is None


def test_loader_watermarks(con):
    wm = state.loader_watermarks(con, 'good', {'bat': 'season', 'pit': 'season'})
    assert wm == {'bat': '2024', 'pit': '2022'}


def test_schema_last_load(con):
    last_load, count = state.schema_last_load(con, 'good')
    assert count == 1 and last_load is not None
    assert state.schema_last_load(con, 'ghost') == (None, 0)


def test_loader_status_fields(con):
    st = state.loader_status(con, _module('good', {'bat': 'season', 'pit': 'season'}))
    assert st.schema == 'good'
    assert st.table_count == 3            # bat, meta, pit (not _dlt_loads)
    assert st.load_count == 1
    assert st.watermarks == {'bat': '2024', 'pit': '2022'}
    assert st.oldest == '2022'            # laggard
    assert st.full_refresh_only is False


def test_loader_status_lagging_table_forces_full_history(con):
    # 'absent' is declared but never loaded => oldest collapses to None.
    st = state.loader_status(con, _module('good', {'bat': 'season', 'absent': 'season'}))
    assert st.watermarks == {'bat': '2024', 'absent': None}
    assert st.oldest is None


def test_loader_status_full_refresh_only(con):
    st = state.loader_status(con, _module('single', {}))
    assert st.full_refresh_only is True
    assert st.oldest is None
    assert st.table_count == 1


def test_loader_status_never_loaded(con):
    st = state.loader_status(con, _module('ghost', {'x': 'season'}))
    assert st.table_count == 0
    assert st.last_load is None and st.load_count == 0
    assert st.oldest is None


# --- formatting + the `buehrle state` command -----------------------------

def test_fmt():
    assert state.fmt(None) == '-'
    assert state.fmt(1234567) == '1,234,567'
    assert state.fmt('2024') == '2024'


def _run_main(monkeypatch, db, mode, modules):
    monkeypatch.setattr('loaders.registry.data_loaders', lambda: modules)
    args = SimpleNamespace(mode=mode, db=db)
    state.main(None, args)


def test_main_schema_mode(monkeypatch, capsys, db):
    modules = [
        _module('good', {'bat': 'season', 'pit': 'season'}),
        _module('single', {}),
        _module('ghost', {'x': 'season'}),
    ]
    _run_main(monkeypatch, db, 'schema', modules)
    out = capsys.readouterr().out
    assert 'loader' in out and 'watermark' in out
    assert '2022' in out          # good's oldest
    lines = [ln for ln in out.splitlines() if ln.startswith('ghost')]
    assert lines and lines[0].split()[-1] == '-'   # never-loaded => '-'


def test_main_table_mode(monkeypatch, capsys, db):
    modules = [
        _module('good', {'bat': 'season', 'pit': 'season'}),
        _module('ghost', {'x': 'season'}),  # no tables => single '-' row
    ]
    _run_main(monkeypatch, db, 'table', modules)
    out = capsys.readouterr().out
    assert 'table' in out
    assert 'bat' in out and 'meta' in out
    # per-table watermark column populated for watermarked tables only
    bat_line = next(ln for ln in out.splitlines() if ln.startswith('good') and ' bat ' in f' {ln} ')
    assert '2024' in bat_line
