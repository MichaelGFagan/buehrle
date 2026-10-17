import io
import subprocess
import tarfile

import pytest

from loaders.retrosheet import install_chadwick


@pytest.fixture
def run_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(
        subprocess, 'run',
        lambda args, **kw: calls.append((args, kw)) or subprocess.CompletedProcess(args=args, returncode=0),
    )
    return calls


def fake_which(available: set[str]):
    return lambda name: f'/usr/bin/{name}' if name in available else None


def test_skips_when_cwevent_already_installed(monkeypatch, capsys):
    monkeypatch.setattr(install_chadwick.shutil, 'which', fake_which({'cwevent'}))
    built = []
    monkeypatch.setattr(install_chadwick, 'install_from_source', lambda: built.append(True))
    install_chadwick.install()
    assert built == []
    assert 'already installed' in capsys.readouterr().out


@pytest.mark.parametrize('platform', ['darwin', 'linux'])
def test_builds_from_source_on_every_platform(monkeypatch, platform):
    monkeypatch.setattr(install_chadwick.shutil, 'which', fake_which({'brew'}))
    monkeypatch.setattr(install_chadwick.sys, 'platform', platform)
    built = []
    monkeypatch.setattr(install_chadwick, 'install_from_source', lambda: built.append(True))
    install_chadwick.install()
    assert built == [True]


def test_force_rebuilds_over_existing(monkeypatch):
    monkeypatch.setattr(install_chadwick.shutil, 'which', fake_which({'cwevent'}))
    built = []
    monkeypatch.setattr(install_chadwick, 'install_from_source', lambda: built.append(True))
    install_chadwick.install(force=True)
    assert built == [True]


def write_fake_tarball(url, destination):
    """Stand-in for urlretrieve: write a tarball with the expected top-level dir."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w:gz') as tarball:
        info = tarfile.TarInfo(f'chadwick-{install_chadwick.CHADWICK_VERSION}/configure')
        info.size = 0
        tarball.addfile(info, io.BytesIO(b''))
    with open(destination, 'wb') as handle:
        handle.write(buffer.getvalue())


def test_install_from_source_runs_static_build(tmp_path, monkeypatch, run_calls, capsys):
    monkeypatch.setattr(install_chadwick.shutil, 'which', fake_which({'make', 'cc'}))
    monkeypatch.setattr(install_chadwick.urllib.request, 'urlretrieve', write_fake_tarball)
    monkeypatch.setenv('PATH', '/usr/bin')

    install_chadwick.install_from_source(prefix=tmp_path)

    commands = [args for args, _ in run_calls]
    assert commands == [
        ['./configure', f'--prefix={tmp_path}', '--disable-shared'],
        ['make'],
        ['make', 'install'],
    ]
    assert all(kw['cwd'].name == f'chadwick-{install_chadwick.CHADWICK_VERSION}' for _, kw in run_calls)
    assert 'not on PATH' in capsys.readouterr().out


@pytest.mark.parametrize('platform, hint', [
    ('linux', 'build-essential'),
    ('darwin', 'xcode-select --install'),
])
def test_install_from_source_exits_without_build_tools(monkeypatch, run_calls, platform, hint):
    monkeypatch.setattr(install_chadwick.shutil, 'which', fake_which({'make'}))
    monkeypatch.setattr(install_chadwick.sys, 'platform', platform)
    with pytest.raises(SystemExit, match=hint):
        install_chadwick.install_from_source()
    assert run_calls == []
