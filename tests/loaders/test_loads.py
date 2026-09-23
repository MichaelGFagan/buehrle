"""Tests for the ``loads`` batch command (loaders/loads.py).

The subprocess runner and DB reads are stubbed; what's exercised here is the
arg -> selection -> job resolution and the concurrency dispatch.
"""

import argparse
import datetime

import pytest

from loaders import loads
from loaders.interactive import core


def _row(command, *, full_refresh_only=False, oldest=None):
    return core.LoaderRow(
        module=None,
        command=command,
        schema=command.replace('-', '_'),
        accepts_dates=False,
        full_refresh_only=full_refresh_only,
        table_count=0,
        last_load=None,
        load_count=0,
        oldest=oldest,
    )


@pytest.fixture
def rows():
    return [
        _row('fangraphs', oldest='2021'),
        _row('lahman', full_refresh_only=True),
        _row('chadwick-register', full_refresh_only=True),
    ]


def _parser():
    return argparse.ArgumentParser()


# --- row selection ---------------------------------------------------------

def test_select_rows_no_names_selects_all(rows):
    assert loads._select_rows(rows, [], _parser()) == rows


def test_select_rows_filters_by_name(rows):
    chosen = loads._select_rows(rows, ['lahman', 'fangraphs'], _parser())
    assert [r.command for r in chosen] == ['lahman', 'fangraphs']


def test_select_rows_unknown_name_errors(rows):
    with pytest.raises(SystemExit):
        loads._select_rows(rows, ['nope'], _parser())


# --- job resolution via main ----------------------------------------------

def _run_main(monkeypatch, rows, **arg_overrides):
    """Drive main() with load_rows/run_jobs stubbed; return the built jobs."""
    captured = {}

    def fake_run_jobs(jobs, write, *, max_workers, base_argv=None):
        captured['jobs'] = jobs
        captured['max_workers'] = max_workers
        return []

    monkeypatch.setattr(loads, 'load_rows', lambda db: rows)
    monkeypatch.setattr('loaders.interactive.runner.run_jobs', fake_run_jobs)
    monkeypatch.setattr('loaders.dlt_utils.resolve_backend',
                        lambda: arg_overrides.pop('backend', 'ducklake'))

    args = argparse.Namespace(
        loaders=arg_overrides.get('loaders', []),
        full_refresh=arg_overrides.get('full_refresh', False),
        max_concurrency=arg_overrides.get('max_concurrency', None),
        sequential=arg_overrides.get('sequential', False),
        db='ignored',
        no_log_file=arg_overrides.get('no_log_file', True),
        log_file=arg_overrides.get('log_file', None),
    )
    loads.main(_parser(), args)
    return captured


def test_incremental_default_builds_expected_jobs(monkeypatch, rows):
    captured = _run_main(monkeypatch, rows)
    labels = [j.label for j in captured['jobs']]
    assert labels == ['fangraphs', 'lahman', 'chadwick-register']
    # incremental-capable loader gets a scoped season backfill...
    fg = captured['jobs'][0]
    assert fg.argv_tail == ['load', 'fangraphs', '--start-season', '2021',
                            '--end-season', str(datetime.date.today().year)]
    # ...full-refresh-only loaders can only full-refresh.
    lahman = captured['jobs'][1]
    assert lahman.argv_tail == ['load', 'lahman', '--full-refresh']


def test_full_refresh_forces_full_on_every_loader(monkeypatch, rows):
    captured = _run_main(monkeypatch, rows, full_refresh=True)
    fg = captured['jobs'][0]
    assert fg.argv_tail == ['load', 'fangraphs', '--full-refresh', '--full-history']


def test_concurrency_capped_on_ducklake(monkeypatch, rows):
    captured = _run_main(monkeypatch, rows, max_concurrency=2)
    assert captured['max_workers'] == 2


def test_sequential_flag_forces_one_worker(monkeypatch, rows):
    captured = _run_main(monkeypatch, rows, sequential=True, max_concurrency=4)
    assert captured['max_workers'] == 1


def test_duckdb_backend_forces_one_worker(monkeypatch, rows):
    captured = _run_main(monkeypatch, rows, backend='duckdb', max_concurrency=4)
    assert captured['max_workers'] == 1


def test_failures_exit_nonzero(monkeypatch, rows):
    monkeypatch.setattr(loads, 'load_rows', lambda db: rows)
    monkeypatch.setattr('loaders.interactive.runner.run_jobs',
                        lambda jobs, write, *, max_workers, base_argv=None: ['lahman'])
    monkeypatch.setattr('loaders.dlt_utils.resolve_backend', lambda: 'ducklake')
    args = argparse.Namespace(loaders=[], full_refresh=False,
                              max_concurrency=None, sequential=False, db='ignored',
                              no_log_file=True, log_file=None)
    with pytest.raises(SystemExit):
        loads.main(_parser(), args)


# --- log file --------------------------------------------------------------

def test_log_file_written_by_default(monkeypatch, tmp_path, rows):
    def fake_run_jobs(jobs, write, *, max_workers, base_argv=None):
        write('[fangraphs] hello from the loader')
        return []

    monkeypatch.setattr(loads, 'load_rows', lambda db: rows)
    monkeypatch.setattr('loaders.interactive.runner.run_jobs', fake_run_jobs)
    monkeypatch.setattr('loaders.dlt_utils.resolve_backend', lambda: 'ducklake')

    log_path = tmp_path / 'run.log'
    args = argparse.Namespace(loaders=['fangraphs'], full_refresh=False,
                              max_concurrency=None, sequential=False, db='ignored',
                              no_log_file=False, log_file=log_path)
    loads.main(_parser(), args)
    text = log_path.read_text()
    assert 'Planned loads:' in text
    assert '[fangraphs] hello from the loader' in text


def test_no_log_file_writes_nothing(monkeypatch, tmp_path, rows):
    monkeypatch.setattr(loads, 'load_rows', lambda db: rows)
    monkeypatch.setattr('loaders.interactive.runner.run_jobs',
                        lambda jobs, write, *, max_workers, base_argv=None: [])
    monkeypatch.setattr('loaders.dlt_utils.resolve_backend', lambda: 'ducklake')

    log_path = tmp_path / 'should-not-exist.log'
    args = argparse.Namespace(loaders=['fangraphs'], full_refresh=False,
                              max_concurrency=None, sequential=False, db='ignored',
                              no_log_file=True, log_file=log_path)
    loads.main(_parser(), args)
    assert not log_path.exists()


def test_default_log_path_is_timestamped(monkeypatch, tmp_path, rows):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(loads, 'load_rows', lambda db: rows)
    monkeypatch.setattr('loaders.interactive.runner.run_jobs',
                        lambda jobs, write, *, max_workers, base_argv=None: [])
    monkeypatch.setattr('loaders.dlt_utils.resolve_backend', lambda: 'ducklake')

    args = argparse.Namespace(loaders=['fangraphs'], full_refresh=False,
                              max_concurrency=None, sequential=False, db='ignored',
                              no_log_file=False, log_file=None)
    loads.main(_parser(), args)
    written = list((tmp_path / 'logs').glob('loads-*.log'))
    assert len(written) == 1
