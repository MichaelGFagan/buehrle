import runpy
import sys

import dlt
import duckdb

from loaders.retrosheet import retrosheet_game_logs

COLUMNS = retrosheet_game_logs.COLUMNS


def _make_row(**values):
    row = ['NA'] * len(COLUMNS)
    for k, v in values.items():
        row[COLUMNS.index(k)] = v
    return ','.join(row)


def _build_pipeline(tmp_path, name='retrosheet_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


def test_pipeline_loads_season_file(tmp_path, monkeypatch):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'GL2024.TXT').write_text(
        _make_row(date='20240401', game_num='0', home_team='NYY', visiting_team='BOS') + '\n'
        + _make_row(date='20240402', game_num='0', home_team='LAD', visiting_team='SF') + '\n'
    )

    monkeypatch.setattr(
        retrosheet_game_logs,
        'SEASON_PATH',
        str(tmp_path / 'seasons' / '{season}' / 'GL{season}.TXT'),
    )
    monkeypatch.setattr(
        retrosheet_game_logs,
        'PLAYOFF_PATH',
        str(tmp_path / 'gamelog' / 'GL{suffix}.TXT'),
    )

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(retrosheet_game_logs.retrosheet(start_season=2024, end_season=2024))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT date, home_team, game_type FROM retrosheet_test.game_logs ORDER BY date'
    ).fetchall()
    assert rows == [
        ('20240401', 'NYY', 'regular_season'),
        ('20240402', 'LAD', 'regular_season'),
    ]


def test_pipeline_pk_columns_non_nullable(tmp_path, monkeypatch):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'GL2024.TXT').write_text(
        _make_row(date='20240401', game_num='0', home_team='NYY') + '\n'
    )

    monkeypatch.setattr(
        retrosheet_game_logs,
        'SEASON_PATH',
        str(tmp_path / 'seasons' / '{season}' / 'GL{season}.TXT'),
    )
    monkeypatch.setattr(
        retrosheet_game_logs,
        'PLAYOFF_PATH',
        str(tmp_path / 'gamelog' / 'GL{suffix}.TXT'),
    )

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(retrosheet_game_logs.retrosheet(start_season=2024, end_season=2024))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    schema = con.execute(
        "SELECT column_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'retrosheet_test' AND table_name = 'game_logs' "
        "AND column_name IN ('home_team', 'date', 'game_num') "
        "ORDER BY column_name"
    ).fetchall()
    assert schema == [
        ('date', 'NO'),
        ('game_num', 'NO'),
        ('home_team', 'NO'),
    ]


def test_warns_for_missing_seasons(tmp_path, monkeypatch):
    monkeypatch.setattr(
        retrosheet_game_logs,
        'SEASON_PATH',
        str(tmp_path / 'seasons' / '{season}' / 'GL{season}.TXT'),
    )
    monkeypatch.setattr(
        retrosheet_game_logs,
        'PLAYOFF_PATH',
        str(tmp_path / 'gamelog' / 'GL{suffix}.TXT'),
    )

    pipeline = _build_pipeline(tmp_path, name='gl_missing')
    # No season files exist anywhere
    pipeline.run(retrosheet_game_logs.retrosheet(start_season=2099, end_season=2099))


def test_loads_successful_playoff_files(tmp_path, monkeypatch):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'GL2024.TXT').write_text(
        _make_row(date='20240401', game_num='0', home_team='NYY') + '\n'
    )

    gamelog_dir = tmp_path / 'gamelog'
    gamelog_dir.mkdir()
    (gamelog_dir / 'GLWS.TXT').write_text(
        _make_row(date='20241025', game_num='0', home_team='NYY') + '\n'
    )

    monkeypatch.setattr(
        retrosheet_game_logs,
        'SEASON_PATH',
        str(tmp_path / 'seasons' / '{season}' / 'GL{season}.TXT'),
    )
    monkeypatch.setattr(
        retrosheet_game_logs,
        'PLAYOFF_PATH',
        str(tmp_path / 'gamelog' / 'GL{suffix}.TXT'),
    )

    pipeline = _build_pipeline(tmp_path, name='gl_playoff')
    pipeline.run(retrosheet_game_logs.retrosheet(start_season=2024, end_season=2024))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        "SELECT game_type FROM gl_playoff.game_logs WHERE date = '20241025'"
    ).fetchall()
    assert rows == [('world_series',)]


def test_main_executes(tmp_path, monkeypatch, fake_make_pipeline):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'GL2024.TXT').write_text(
        _make_row(date='20240401', game_num='0', home_team='NYY') + '\n'
    )

    monkeypatch.setattr('loaders.retrosheet.retrosheet_sync.REPO_DIR', str(tmp_path))
    monkeypatch.setattr('loaders.retrosheet.retrosheet_sync.check', lambda: None)
    monkeypatch.setattr('loaders.dlt_utils.make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['retrosheet_game_logs', '--start', '2024', '--end', '2024', '--full-refresh'])
    runpy.run_module('loaders.retrosheet.retrosheet_game_logs', run_name='__main__')
