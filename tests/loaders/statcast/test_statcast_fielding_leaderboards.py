import re
import sys

import dlt
import duckdb
import responses

import loaders.__main__ as loaders_main
from loaders.statcast import _common
from loaders.statcast import statcast_fielding_leaderboards as fielding_mod
from loaders.statcast.statcast_fielding_leaderboards import statcast_fielding_leaderboards

STUB_BODY = 'player_id,pitcher,pitcher_id,entity_id,resp_fielder_id,id,pitch_type\nstub,stub,stub,stub,stub,stub,FF'

BASE_URL = _common.BASE_URL


def _build_pipeline(tmp_path, name='statcast_fielding_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


@responses.activate
def test_pipeline_loads_outfield_catch_prob(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(
        responses.GET,
        f'{BASE_URL}/catch_probability?type=player&min=0&year=2023&total=&csv=true',
        body='player_id,catch_pct\n100,0.95\n101,0.88',
        status=200,
    )

    pipeline = _build_pipeline(tmp_path)
    source = statcast_fielding_leaderboards(2023, 2023).with_resources('outfield_catch_prob')
    pipeline.run(source)

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT year, player_id, catch_pct FROM statcast_fielding_test.outfield_catch_prob '
        'ORDER BY player_id'
    ).fetchall()
    assert rows == [('2023', '100', '0.95'), ('2023', '101', '0.88')]


@responses.activate
def test_pipeline_loads_all_resources(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(responses.GET, re.compile(r'.*'), body=STUB_BODY, status=200)

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(statcast_fielding_leaderboards(2023, 2023))


@responses.activate
def test_main_executes(monkeypatch, fake_make_pipeline):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(
        responses.GET,
        f'{BASE_URL}/catch_probability?type=player&min=0&year=2023&total=&csv=true',
        body='player_id,catch_pct\n100,0.95',
        status=200,
    )

    monkeypatch.setattr(fielding_mod, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'buehrle', 'statcast-fielding', '--season', '2023',
        '--resources', 'outfield_catch_prob', '--full-refresh',
    ])
    loaders_main.main()
