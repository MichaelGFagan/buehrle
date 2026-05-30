import re
import sys

import dlt
import duckdb
import responses

import loaders.__main__ as loaders_main
from loaders.dlt_utils import handle_full_refresh
from loaders.statcast import _common
from loaders.statcast import statcast_batting_leaderboards as batting_mod
from loaders.statcast.statcast_batting_leaderboards import statcast_batting_leaderboards

STUB_BODY = 'player_id,pitcher,pitcher_id,entity_id,resp_fielder_id,id,pitch_type\nstub,stub,stub,stub,stub,stub,FF'

BASE_URL = _common.BASE_URL


def _mock_year(year: int, body: str):
    responses.add(
        responses.GET,
        f'{BASE_URL}/statcast?type=batter&year={year}&position=&team=&min=1&csv=true',
        body=body,
        status=200,
    )


def _build_pipeline(tmp_path, name='statcast_batting_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


@responses.activate
def test_pipeline_loads_multiple_years(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    _mock_year(2023, 'player_id,abs\n100,500\n101,400')
    _mock_year(2024, 'player_id,abs\n100,520\n101,420')

    pipeline = _build_pipeline(tmp_path)
    source = statcast_batting_leaderboards(2023, 2024).with_resources('exit_velo_barrels')
    pipeline.run(source)

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT year, player_id, abs FROM statcast_batting_test.exit_velo_barrels '
        'ORDER BY year, player_id'
    ).fetchall()
    assert rows == [
        ('2023', '100', '500'),
        ('2023', '101', '400'),
        ('2024', '100', '520'),
        ('2024', '101', '420'),
    ]


@responses.activate
def test_state_advances_to_last_fetched_year(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    _mock_year(2023, 'player_id,abs\n100,500')
    _mock_year(2024, 'player_id,abs\n200,600')

    pipeline = _build_pipeline(tmp_path)
    source = statcast_batting_leaderboards(2023, 2024).with_resources('exit_velo_barrels')
    pipeline.run(source)

    state = pipeline.state['sources']['statcast_batting_leaderboards']['resources']['exit_velo_barrels']
    assert state['last_year'] == 2024


@responses.activate
def test_update_flag_bypasses_state(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    _mock_year(2020, 'player_id,abs\n100,500')

    pipeline = _build_pipeline(tmp_path)
    source = statcast_batting_leaderboards(2020, 2020).with_resources('exit_velo_barrels')
    pipeline.run(source)

    responses.reset()
    _mock_year(2024, 'player_id,abs\n200,600')

    source2 = statcast_batting_leaderboards(2024, 2024, update=True).with_resources('exit_velo_barrels')
    pipeline.run(source2)

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT year, player_id FROM statcast_batting_test.exit_velo_barrels ORDER BY year, player_id'
    ).fetchall()
    assert rows == [('2020', '100'), ('2024', '200')]


@responses.activate
def test_full_refresh_resets_state_and_destination(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    _mock_year(2023, 'player_id,abs\n100,500')

    pipeline = _build_pipeline(tmp_path)
    source = statcast_batting_leaderboards(2023, 2023).with_resources('exit_velo_barrels')
    pipeline.run(source)

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    assert con.execute(
        'SELECT COUNT(*) FROM statcast_batting_test.exit_velo_barrels'
    ).fetchone()[0] == 1
    con.close()

    handle_full_refresh(pipeline)

    _mock_year(2023, 'player_id,abs\n200,600')
    source2 = statcast_batting_leaderboards(2023, 2023).with_resources('exit_velo_barrels')
    pipeline.run(source2)

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT player_id FROM statcast_batting_test.exit_velo_barrels'
    ).fetchall()
    assert rows == [('200',)]


@responses.activate
def test_pipeline_loads_all_resources(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(responses.GET, re.compile(r'.*'), body=STUB_BODY, status=200)

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(statcast_batting_leaderboards(2023, 2023))


@responses.activate
def test_main_executes(monkeypatch, fake_make_pipeline):
    monkeypatch.setattr('time.sleep', lambda s: None)
    _mock_year(2023, 'player_id,abs\n100,500')

    monkeypatch.setattr(batting_mod, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'buehrle', 'statcast-batting', '--season', '2023',
        '--resources', 'exit_velo_barrels', '--full-refresh',
    ])
    loaders_main.main()
