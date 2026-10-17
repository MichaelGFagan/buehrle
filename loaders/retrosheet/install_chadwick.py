#!/usr/bin/env python3
"""
Install Chadwick Bureau tools (cwevent, cwgame, etc.) by building the pinned
release from source into ~/.local, so no sudo is needed.

Building from source on every OS keeps the version identical across machines.
The build is static (--disable-shared), so cwevent doesn't need
LD_LIBRARY_PATH to find libchadwick at runtime - important when it's run from
a systemd unit.
"""
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

CHADWICK_VERSION = '0.11.0'
TARBALL_URL = (
    f'https://github.com/chadwickbureau/chadwick/releases/download/'
    f'v{CHADWICK_VERSION}/chadwick-{CHADWICK_VERSION}.tar.gz'
)
DEFAULT_PREFIX = Path.home() / '.local'
BUILD_TOOLS = ('make', 'cc')


def build_tools_hint() -> str:
    if sys.platform == 'darwin':
        return 'On macOS run: xcode-select --install'
    return 'On Debian/Ubuntu run: sudo apt install build-essential'


def install_from_source(prefix: Path = DEFAULT_PREFIX) -> None:
    missing = [tool for tool in BUILD_TOOLS if not shutil.which(tool)]
    if missing:
        sys.exit(f'Missing build tools: {", ".join(missing)}.\n{build_tools_hint()}')

    with tempfile.TemporaryDirectory() as tmp:
        tarball_path = Path(tmp) / 'chadwick.tar.gz'
        print(f'Downloading {TARBALL_URL}')
        urllib.request.urlretrieve(TARBALL_URL, tarball_path)
        with tarfile.open(tarball_path) as tarball:
            tarball.extractall(tmp, filter='data')

        source_dir = Path(tmp) / f'chadwick-{CHADWICK_VERSION}'
        subprocess.run(
            ['./configure', f'--prefix={prefix}', '--disable-shared'],
            cwd=source_dir, check=True,
        )
        subprocess.run(['make'], cwd=source_dir, check=True)
        subprocess.run(['make', 'install'], cwd=source_dir, check=True)

    bin_dir = prefix / 'bin'
    print(f'Installed Chadwick {CHADWICK_VERSION} into {bin_dir}')
    if str(bin_dir) not in os.environ.get('PATH', '').split(os.pathsep):
        print(f'{bin_dir} is not on PATH. Log out and back in, or add it to PATH.')


def install(force: bool = False) -> None:
    existing = shutil.which('cwevent')
    if existing and not force:
        print(f'cwevent already installed at {existing}. Use --force to reinstall.')
        return
    install_from_source()


def register(subparsers):
    parser = subparsers.add_parser(
        'install-chadwick',
        help=f'Build Chadwick {CHADWICK_VERSION} from source into ~/.local/bin',
    )
    parser.add_argument('--force', action='store_true', help='Reinstall even if cwevent is already on PATH.')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    install(force=args.force)
