import re
import sys

import dlt
import duckdb
import responses

import loaders.__main__ as loaders_main
from loaders.statcast import _common
from loaders.statcast import statcast_running_leaderboards as running_mod
from loaders.statcast.statcast_running_leaderboards import statcast_running_leaderboards

STUB_BODY = 'player_id,pitcher,pitcher_id,entity_id,resp_fielder_id,id,pitch_type\nstub,stub,stub,stub,stub,stub,FF'

BASE_URL = _common.BASE_URL


def _build_pipeline(tmp_path, name='statcast_running_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


@responses.activate
def test_pipeline_loads_sprint_speed(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(
        responses.GET,
        f'{BASE_URL}/sprint_speed?year=2023&position=&team=&min=0&csv=true',
        body='player_id,sprint_speed\n100,28.5\n101,27.9',
        status=200,
    )

    pipeline = _build_pipeline(tmp_path)
    source = statcast_running_leaderboards(2023, 2023).with_resources('sprint_speed')
    pipeline.run(source)

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT year, player_id, sprint_speed FROM statcast_running_test.sprint_speed '
        'ORDER BY player_id'
    ).fetchall()
    assert rows == [('2023', '100', '28.5'), ('2023', '101', '27.9')]


@responses.activate
def test_pipeline_loads_all_resources(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(responses.GET, re.compile(r'.*'), body=STUB_BODY, status=200)

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(statcast_running_leaderboards(2023, 2023))


@responses.activate
def test_run_years_skips_empty_csv_response(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    # header-only CSV → fetch_csv returns None → run_years continues
    responses.add(responses.GET, re.compile(r'.*'), body='player_id\n', status=200)

    pipeline = _build_pipeline(tmp_path, name='running_empty')
    pipeline.run(statcast_running_leaderboards(2023, 2023).with_resources('sprint_speed'))


@responses.activate
def test_main_executes(monkeypatch, fake_make_pipeline):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(
        responses.GET,
        f'{BASE_URL}/sprint_speed?year=2023&position=&team=&min=0&csv=true',
        body='player_id,sprint_speed\n100,28.5',
        status=200,
    )

    monkeypatch.setattr(running_mod, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'buehrle', 'statcast-running', '--season', '2023',
        '--resources', 'sprint_speed', '--full-refresh',
    ])
    loaders_main.main()
