import re
import sys

import dlt
import duckdb
import responses

import loaders.__main__ as loaders_main
from loaders.statcast import _common
from loaders.statcast import statcast_pitching_leaderboards as pitching_mod
from loaders.statcast.statcast_pitching_leaderboards import statcast_pitching_leaderboards

STUB_BODY = 'player_id,pitcher,pitcher_id,entity_id,resp_fielder_id,id,pitch_type\nstub,stub,stub,stub,stub,stub,FF'

BASE_URL = _common.BASE_URL


def _build_pipeline(tmp_path, name='statcast_pitching_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


@responses.activate
def test_pipeline_loads_exit_velo_barrels(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(
        responses.GET,
        f'{BASE_URL}/statcast?type=pitcher&year=2023&position=&team=&min=1&csv=true',
        body='player_id,barrels\n200,12\n201,7',
        status=200,
    )

    pipeline = _build_pipeline(tmp_path)
    source = statcast_pitching_leaderboards(2023, 2023).with_resources('exit_velo_barrels')
    pipeline.run(source)

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT year, player_id, barrels FROM statcast_pitching_test.exit_velo_barrels '
        'ORDER BY player_id'
    ).fetchall()
    assert rows == [('2023', '200', '12'), ('2023', '201', '7')]


@responses.activate
def test_pipeline_loads_all_resources(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(responses.GET, re.compile(r'.*'), body=STUB_BODY, status=200)

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(statcast_pitching_leaderboards(2023, 2023))


@responses.activate
def test_main_executes(monkeypatch, fake_make_pipeline):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(
        responses.GET,
        f'{BASE_URL}/statcast?type=pitcher&year=2023&position=&team=&min=1&csv=true',
        body='player_id,barrels\n200,12',
        status=200,
    )

    monkeypatch.setattr(pitching_mod, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'buehrle', 'load', 'statcast-pitching', '--season', '2023',
        '--resources', 'exit_velo_barrels', '--full-refresh',
    ])
    loaders_main.main()
