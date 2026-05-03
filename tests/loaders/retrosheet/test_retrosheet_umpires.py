import runpy
import sys

import dlt
import duckdb

from loaders.retrosheet import retrosheet_umpires


def _build_pipeline(tmp_path, name='retrosheet_umpires_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


def test_pipeline_loads_umpires_file(tmp_path, monkeypatch):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'UMPIRES2024.txt').write_text(
        'ID,last,first\n'
        'westj001,West,Joe\n'
        'iassd001,Iassogna,Dan\n'
    )

    monkeypatch.setattr(retrosheet_umpires, 'REPO_DIR', str(tmp_path))

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(retrosheet_umpires.retrosheet_umpires(start_season=2024, end_season=2024))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT umpire_id, last_name, season FROM retrosheet_umpires_test.umpires '
        'ORDER BY umpire_id'
    ).fetchall()
    assert rows == [
        ('iassd001', 'Iassogna', '2024'),
        ('westj001', 'West', '2024'),
    ]


def test_warns_for_missing_umpires_files(tmp_path, monkeypatch):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'UMPIRES2024.txt').write_text('ID,last,first\nwestj001,West,Joe\n')

    monkeypatch.setattr(retrosheet_umpires, 'REPO_DIR', str(tmp_path))

    pipeline = _build_pipeline(tmp_path, name='umpires_warn')
    pipeline.run(retrosheet_umpires.retrosheet_umpires(start_season=2024, end_season=2026))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute('SELECT umpire_id FROM umpires_warn.umpires').fetchall()
    assert rows == [('westj001',)]  # 2025, 2026 skipped (missing files)


def test_main_executes(tmp_path, monkeypatch, fake_make_pipeline):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'UMPIRES2024.txt').write_text(
        'ID,last,first\nwestj001,West,Joe\n'
    )

    monkeypatch.setattr('loaders.retrosheet.retrosheet_sync.REPO_DIR', str(tmp_path))
    monkeypatch.setattr('loaders.retrosheet.retrosheet_sync.check', lambda: None)
    monkeypatch.setattr('loaders.dlt_utils.make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['retrosheet_umpires', '--start', '2024', '--end', '2024', '--full-refresh'])
    runpy.run_module('loaders.retrosheet.retrosheet_umpires', run_name='__main__')
