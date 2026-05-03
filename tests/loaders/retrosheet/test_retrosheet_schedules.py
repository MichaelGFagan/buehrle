import runpy
import sys

import dlt
import duckdb

from loaders.retrosheet import retrosheet_schedules


def _build_pipeline(tmp_path, name='retrosheet_schedules_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


def test_pipeline_loads_schedule_file(tmp_path, monkeypatch):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / '2024schedule.csv').write_text(
        '20240401,0,Mon,BOS,AL,1,NYY,AL,1,D,,\n'
        '20240402,0,Tue,SF,NL,1,LAD,NL,1,N,,\n'
    )

    monkeypatch.setattr(
        retrosheet_schedules,
        'SCHEDULE_PATH',
        str(tmp_path / 'seasons' / '{season}' / '{season}schedule.csv'),
    )

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(retrosheet_schedules.retrosheet_schedules(start_season=2024, end_season=2024))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT date, home_team FROM retrosheet_schedules_test.schedules ORDER BY date'
    ).fetchall()
    assert rows == [('20240401', 'NYY'), ('20240402', 'LAD')]


def test_warns_for_missing_schedule_files(tmp_path, monkeypatch):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / '2024schedule.csv').write_text(
        '20240401,0,Mon,BOS,AL,1,NYY,AL,1,D,,\n'
    )

    monkeypatch.setattr(
        retrosheet_schedules,
        'SCHEDULE_PATH',
        str(tmp_path / 'seasons' / '{season}' / '{season}schedule.csv'),
    )

    pipeline = _build_pipeline(tmp_path, name='schedules_warn')
    pipeline.run(retrosheet_schedules.retrosheet_schedules(start_season=2024, end_season=2026))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute('SELECT date FROM schedules_warn.schedules').fetchall()
    assert rows == [('20240401',)]  # 2025, 2026 skipped (missing files)


def test_main_executes(tmp_path, monkeypatch, fake_make_pipeline):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / '2024schedule.csv').write_text(
        '20240401,0,Mon,BOS,AL,1,NYY,AL,1,D,,\n'
    )

    monkeypatch.setattr('loaders.retrosheet.retrosheet_sync.REPO_DIR', str(tmp_path))
    monkeypatch.setattr('loaders.retrosheet.retrosheet_sync.sync', lambda: None)
    monkeypatch.setattr('loaders.dlt_utils.make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['retrosheet_schedules', '--start', '2024', '--end', '2024', '--full-refresh'])
    runpy.run_module('loaders.retrosheet.retrosheet_schedules', run_name='__main__')
