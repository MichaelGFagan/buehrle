import datetime
import runpy
import sys

import dlt
import duckdb
import polars as pl
import pyarrow as pa
import pytest
import responses

from loaders.dlt_utils import to_arrow
from loaders.statcast import statcast_pitches as sp

URL = sp.BASE_STATCAST_URL


@pytest.mark.parametrize('s, end, expected', [
    ('2024', False, datetime.date(2024, 3, 1)),
    ('2024', True, datetime.date(2024, 12, 31)),
    ('2024-05-15', False, datetime.date(2024, 5, 15)),
    ('2024-05-15', True, datetime.date(2024, 5, 15)),
])
def test_parse_date(s, end, expected):
    assert sp._parse_date(s, end=end) == expected


@responses.activate
def test_fetch_range_returns_arrow_with_renamed_columns():
    responses.add(
        responses.GET, URL,
        body='game_pk,at_bat_number,pitch_number,pitcher.1,fielder_2.1\n718000,1,1,500,600',
        status=200,
    )
    table = sp._fetch_range(datetime.date(2024, 4, 1), datetime.date(2024, 4, 30))
    assert isinstance(table, pa.Table)
    assert 'pitcher_1' in table.column_names
    assert 'fielder_2_1' in table.column_names
    assert 'pitcher.1' not in table.column_names


def _build_pipeline(tmp_path, name='statcast_pitches_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


@pytest.fixture
def stub_fetch(monkeypatch):
    """Replace _fetch_range with a counter that returns one row per call."""
    calls = []

    def fake(start, end):
        calls.append((start, end))
        df = pl.DataFrame({
            'game_pk': [f'game_{len(calls)}'],
            'at_bat_number': ['1'],
            'pitch_number': ['1'],
        })
        return to_arrow(df, sp.PRIMARY_KEYS)

    monkeypatch.setattr(sp, '_fetch_range', fake)
    return calls


def test_pipeline_loads_single_month(tmp_path, stub_fetch):
    pipeline = _build_pipeline(tmp_path)
    pipeline.run(sp.statcast_source(datetime.date(2024, 4, 1), datetime.date(2024, 4, 30)))

    assert stub_fetch == [(datetime.date(2024, 4, 1), datetime.date(2024, 4, 30))]
    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute('SELECT game_pk FROM statcast_pitches_test.pitches').fetchall()
    assert rows == [('game_1',)]


def test_pipeline_iterates_across_year_boundary(tmp_path, stub_fetch):
    pipeline = _build_pipeline(tmp_path)
    pipeline.run(sp.statcast_source(datetime.date(2024, 11, 15), datetime.date(2025, 2, 28)))

    assert stub_fetch == [
        (datetime.date(2024, 11, 15), datetime.date(2024, 11, 30)),
        (datetime.date(2024, 12, 1), datetime.date(2024, 12, 31)),
        (datetime.date(2025, 1, 1), datetime.date(2025, 1, 31)),
        (datetime.date(2025, 2, 1), datetime.date(2025, 2, 28)),
    ]


def test_state_advances_to_last_month_end(tmp_path, stub_fetch):
    pipeline = _build_pipeline(tmp_path)
    pipeline.run(sp.statcast_source(datetime.date(2024, 4, 1), datetime.date(2024, 5, 15)))

    state = pipeline.state['sources']['statcast_source']['resources']['pitches']
    assert state['last_date'] == '2024-05-15'


def test_resumes_from_state_when_update_not_set(tmp_path, stub_fetch):
    pipeline = _build_pipeline(tmp_path)
    pipeline.run(sp.statcast_source(datetime.date(2024, 4, 1), datetime.date(2024, 4, 30)))
    assert len(stub_fetch) == 1

    # state['last_date']='2024-04-30'. Second run without update should resume from there,
    # not from start_date=Jan 1.
    pipeline.run(sp.statcast_source(datetime.date(2024, 1, 1), datetime.date(2024, 5, 31)))
    assert stub_fetch[1:] == [
        (datetime.date(2024, 4, 30), datetime.date(2024, 4, 30)),
        (datetime.date(2024, 5, 1), datetime.date(2024, 5, 31)),
    ]


def test_update_flag_bypasses_state(tmp_path, stub_fetch):
    pipeline = _build_pipeline(tmp_path)
    pipeline.run(sp.statcast_source(datetime.date(2024, 4, 1), datetime.date(2024, 4, 30)))
    assert len(stub_fetch) == 1

    pipeline.run(sp.statcast_source(
        datetime.date(2024, 1, 1), datetime.date(2024, 4, 30), update=True,
    ))
    # update=True restarts from Jan 1; without it state would resume from Apr 30.
    assert len(stub_fetch) == 5


@responses.activate
def test_main_executes(monkeypatch, fake_make_pipeline):
    responses.add(
        responses.GET, URL,
        body='game_pk,at_bat_number,pitch_number\n1,1,1',
        status=200,
    )

    monkeypatch.setattr('loaders.dlt_utils.make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'statcast_pitches', '--start', '2024-04-01', '--end', '2024-04-15', '--full-refresh',
    ])
    runpy.run_module('loaders.statcast.statcast_pitches', run_name='__main__')
