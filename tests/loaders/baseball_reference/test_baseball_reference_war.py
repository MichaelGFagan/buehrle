import runpy
import sys

import dlt
import duckdb
import responses

from loaders.baseball_reference.baseball_reference_war import URLS, baseball_reference_war


def _build_pipeline(tmp_path, name='bbref_war_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


@responses.activate
def test_pipeline_loads_war_batting_and_pitching(tmp_path):
    responses.add(
        responses.GET,
        URLS['batting'],
        body='name_common,age,WAR\nMike Trout,28,8.5',
        status=200,
    )
    responses.add(
        responses.GET,
        URLS['pitching'],
        body='name_common,age,WAR\nJake deGrom,30,7.2',
        status=200,
    )

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(baseball_reference_war())

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    bat = con.execute('SELECT name_common, war FROM bbref_war_test.war_batting').fetchall()
    pit = con.execute('SELECT name_common, war FROM bbref_war_test.war_pitching').fetchall()
    assert bat == [('Mike Trout', 8.5)]
    assert pit == [('Jake deGrom', 7.2)]


@responses.activate
def test_main_executes(monkeypatch, fake_make_pipeline):
    responses.add(responses.GET, URLS['batting'], body='name_common,age,WAR\nMike Trout,28,8.5', status=200)
    responses.add(responses.GET, URLS['pitching'], body='name_common,age,WAR\nJake deGrom,30,7.2', status=200)

    monkeypatch.setattr('loaders.dlt_utils.make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['baseball_reference_war'])
    runpy.run_module('loaders.baseball_reference.baseball_reference_war', run_name='__main__')
