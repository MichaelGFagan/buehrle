import sys

import dlt
import duckdb

import loaders.__main__ as loaders_main
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


def test_strips_leaked_header_and_loads_location(tmp_path, monkeypatch):
    # Modern (2024+) files carry a header row and a 13th `Location` column.
    # The header must be dropped (not ingested as a 'Date' row) and the park
    # mapped to `location`.
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / '2024schedule.csv').write_text(
        'Date,Num,Day,Visitor,League,Game,Home,League,Game,Day/Night,Location,Postponed,Makeup\n'
        '20240401,0,Mon,BOS,AL,1,NYY,AL,1,D,NYC01,,\n'
        '20240402,0,Tue,SF,NL,1,LAD,NL,1,N,LOS03,,\n'
    )

    monkeypatch.setattr(
        retrosheet_schedules,
        'SCHEDULE_PATH',
        str(tmp_path / 'seasons' / '{season}' / '{season}schedule.csv'),
    )

    pipeline = _build_pipeline(tmp_path, name='schedules_modern')
    pipeline.run(retrosheet_schedules.retrosheet_schedules(start_season=2024, end_season=2024))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT date, home_team, location FROM schedules_modern.schedules ORDER BY date'
    ).fetchall()
    assert rows == [('20240401', 'NYY', 'NYC01'), ('20240402', 'LAD', 'LOS03')]
    # The header row never leaked in as data.
    assert con.execute(
        "SELECT count(*) FROM schedules_modern.schedules WHERE date = 'Date'"
    ).fetchone() == (0,)


def test_old_format_null_fills_location(tmp_path, monkeypatch):
    # Pre-2024 files have a header but only 12 columns (no `Location`).
    season_dir = tmp_path / 'seasons' / '2000'
    season_dir.mkdir(parents=True)
    (season_dir / '2000schedule.csv').write_text(
        'Date,Num,Day,Visitor,League,Game,Home,League,Game,Day/Night,Postponed,Makeup\n'
        '20000401,0,Sat,BOS,AL,1,NYY,AL,1,D,,\n'
    )

    monkeypatch.setattr(
        retrosheet_schedules,
        'SCHEDULE_PATH',
        str(tmp_path / 'seasons' / '{season}' / '{season}schedule.csv'),
    )

    pipeline = _build_pipeline(tmp_path, name='schedules_old')
    pipeline.run(retrosheet_schedules.retrosheet_schedules(start_season=2000, end_season=2000))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT date, home_team, location FROM schedules_old.schedules'
    ).fetchall()
    assert rows == [('20000401', 'NYY', None)]


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
    monkeypatch.setattr(retrosheet_schedules, 'sync', lambda: None)
    monkeypatch.setattr(retrosheet_schedules, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['buehrle', 'retrosheet-schedules', '--season', '2024', '--full-refresh'])
    loaders_main.main()
