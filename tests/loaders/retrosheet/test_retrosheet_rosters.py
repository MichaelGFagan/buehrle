import sys

import dlt
import duckdb

import loaders.__main__ as loaders_main
from loaders.retrosheet import retrosheet_rosters


def _build_pipeline(tmp_path, name='retrosheet_rosters_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


def test_pipeline_loads_roster_files(tmp_path, monkeypatch):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'NYY2024.ROS').write_text(
        'troum001,Trout,Mike,R,R,NYY,CF\n'
        'judga001,Judge,Aaron,R,R,NYY,RF\n'
    )
    (season_dir / 'BOS2024.ROS').write_text(
        'devra001,Devers,Rafael,L,R,BOS,3B\n'
    )

    monkeypatch.setattr(retrosheet_rosters, 'REPO_DIR', str(tmp_path))

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(retrosheet_rosters.retrosheet_rosters(start_season=2024, end_season=2024))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT player_id, team, season FROM retrosheet_rosters_test.rosters '
        'ORDER BY player_id'
    ).fetchall()
    assert rows == [
        ('devra001', 'BOS', '2024'),
        ('judga001', 'NYY', '2024'),
        ('troum001', 'NYY', '2024'),
    ]


def test_warns_for_missing_or_empty_seasons(tmp_path, monkeypatch):
    season_2024 = tmp_path / 'seasons' / '2024'
    season_2024.mkdir(parents=True)
    (season_2024 / 'NYY2024.ROS').write_text('troum001,Trout,Mike,R,R,NYY,CF\n')
    (tmp_path / 'seasons' / '2025').mkdir()  # empty dir, no .ROS files

    monkeypatch.setattr(retrosheet_rosters, 'REPO_DIR', str(tmp_path))

    pipeline = _build_pipeline(tmp_path, name='rosters_warn')
    pipeline.run(retrosheet_rosters.retrosheet_rosters(start_season=2024, end_season=2026))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute('SELECT player_id FROM rosters_warn.rosters').fetchall()
    assert rows == [('troum001',)]  # only 2024 loaded; 2025 (empty) and 2026 (missing) skipped


def test_main_executes(tmp_path, monkeypatch, fake_make_pipeline):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'NYY2024.ROS').write_text('troum001,Trout,Mike,R,R,NYY,CF\n')

    monkeypatch.setattr('loaders.retrosheet.retrosheet_sync.REPO_DIR', str(tmp_path))
    monkeypatch.setattr(retrosheet_rosters, 'check', lambda: None)
    monkeypatch.setattr(retrosheet_rosters, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['buehrle', 'retrosheet-rosters', '--season', '2024', '--full-refresh'])
    loaders_main.main()
