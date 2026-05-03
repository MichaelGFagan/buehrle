import subprocess

import pytest

from loaders.retrosheet import retrosheet_sync


def test_check_passes_when_repo_present(tmp_path, monkeypatch):
    (tmp_path / '.git').mkdir()
    monkeypatch.setattr(retrosheet_sync, 'REPO_DIR', str(tmp_path))
    retrosheet_sync.check()


def test_check_exits_when_repo_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(retrosheet_sync, 'REPO_DIR', str(tmp_path / 'nonexistent'))
    with pytest.raises(SystemExit):
        retrosheet_sync.check()


def test_sync_clones_when_repo_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(retrosheet_sync, 'REPO_DIR', str(tmp_path / 'fake'))

    calls = []
    monkeypatch.setattr(
        subprocess, 'run',
        lambda args, **kw: calls.append(args) or subprocess.CompletedProcess(args=args, returncode=0),
    )

    retrosheet_sync.sync()
    assert calls[0][0] == 'git'
    assert 'clone' in calls[0]


def test_sync_pulls_when_repo_present(tmp_path, monkeypatch):
    (tmp_path / '.git').mkdir()
    monkeypatch.setattr(retrosheet_sync, 'REPO_DIR', str(tmp_path))

    calls = []
    monkeypatch.setattr(
        subprocess, 'run',
        lambda args, **kw: calls.append(args) or subprocess.CompletedProcess(args=args, returncode=0),
    )

    retrosheet_sync.sync()
    assert 'pull' in calls[0]


def test_main_invokes_sync(tmp_path, monkeypatch):
    import runpy
    (tmp_path / '.git').mkdir()
    monkeypatch.setattr(retrosheet_sync, 'REPO_DIR', str(tmp_path))
    monkeypatch.setattr(
        subprocess, 'run',
        lambda args, **kw: subprocess.CompletedProcess(args=args, returncode=0),
    )
    runpy.run_module('loaders.retrosheet.retrosheet_sync', run_name='__main__')
