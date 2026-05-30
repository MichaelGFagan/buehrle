import shutil
import subprocess
import sys

import dlt
import duckdb
import pyarrow as pa
import pytest

import loaders.__main__ as loaders_main
from loaders.retrosheet import retrosheet_events as ev


@pytest.mark.parametrize('extensions, expected', [
    (ev.FULL_EXTENSIONS, ['NYY2024.EVA', 'BOS2024.EVN']),
    (ev.BOX_EXTENSIONS, ['NYY2024.EBA']),
    (ev.DEDUCED_EXTENSIONS, []),
])
def test_event_files_filters_by_extension(tmp_path, extensions, expected):
    for name in ['NYY2024.EVA', 'BOS2024.EVN', 'NYY2024.EBA', 'README.txt']:
        (tmp_path / name).touch()
    assert sorted(ev._event_files(str(tmp_path), extensions)) == sorted(expected)


def test_event_files_matches_case_insensitively(tmp_path):
    (tmp_path / 'NYY2024.eva').touch()
    (tmp_path / 'NYY2024.Evn').touch()
    assert sorted(ev._event_files(str(tmp_path), ev.FULL_EXTENSIONS)) == ['NYY2024.Evn', 'NYY2024.eva']


def test_seasons_yields_existing_directories_only(tmp_path, monkeypatch):
    (tmp_path / '2023').mkdir()
    (tmp_path / '2025').mkdir()
    # 2024 deliberately missing
    monkeypatch.setattr(ev, 'SEASONS_DIR', str(tmp_path))

    result = list(ev._seasons(2023, 2025))
    assert [year for year, _ in result] == [2023, 2025]


def _completed(stdout='', returncode=0):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr='')


def test_run_cwevent_returns_none_on_non_zero_returncode(monkeypatch):
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: _completed(returncode=1))
    assert ev._run_cwevent(2024, '/fake', ['x.EVA']) is None


def test_run_cwevent_returns_none_on_empty_stdout(monkeypatch):
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: _completed(stdout='   \n'))
    assert ev._run_cwevent(2024, '/fake', ['x.EVA']) is None


def test_run_cwevent_parses_stdout_and_adds_event_id(monkeypatch):
    stdout = (
        'GAME_ID,BAT_ID\n'
        'NYY202404010,trout\n'
        'NYY202404010,judge\n'
        'NYY202404020,devers\n'
    )
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: _completed(stdout=stdout))

    table = ev._run_cwevent(2024, '/fake', ['x.EVA'])
    assert isinstance(table, pa.Table)
    assert 'EVENT_ID' in table.column_names
    rows = table.to_pylist()
    assert [r['EVENT_ID'] for r in rows] == ['0', '1', '0']  # restarts per GAME_ID


def _build_pipeline(tmp_path, name='retrosheet_events_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


def test_pipeline_loads_event_data(tmp_path, monkeypatch):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'NYY2024.EVA').touch()

    monkeypatch.setattr(ev, 'SEASONS_DIR', str(tmp_path / 'seasons'))
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: _completed(
        stdout='GAME_ID,BAT_ID\nNYY202404010,trout\nNYY202404010,judge\n',
    ))

    pipeline = _build_pipeline(tmp_path)
    pipeline.run(ev.retrosheet_events(start_season=2024, end_season=2024)
                 .with_resources('retrosheet_game_logs_full'))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT "GAME_ID", "EVENT_ID", "BAT_ID" FROM retrosheet_events_test.retrosheet_game_logs_full '
        'ORDER BY "EVENT_ID"'
    ).fetchall()
    assert rows == [('NYY202404010', '0', 'trout'), ('NYY202404010', '1', 'judge')]


def test_main_exits_when_cwevent_missing(monkeypatch):
    monkeypatch.setattr(shutil, 'which', lambda cmd: None)
    monkeypatch.setattr(sys, 'argv', ['buehrle', 'retrosheet-events'])
    with pytest.raises(SystemExit):
        loaders_main.main()


def test_main_executes(tmp_path, monkeypatch, fake_make_pipeline):
    season_dir = tmp_path / 'seasons' / '2024'
    season_dir.mkdir(parents=True)
    (season_dir / 'NYY2024.EVA').touch()

    monkeypatch.setattr(shutil, 'which', lambda cmd: '/usr/bin/cwevent')
    monkeypatch.setattr('loaders.retrosheet.retrosheet_sync.REPO_DIR', str(tmp_path))
    monkeypatch.setattr(ev, 'check', lambda: None)
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: _completed(
        stdout='GAME_ID,BAT_ID\nNYY202404010,trout\n',
    ))
    monkeypatch.setattr(ev, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['buehrle', 'retrosheet-events', '--season', '2024', '--full-refresh'])
    loaders_main.main()
