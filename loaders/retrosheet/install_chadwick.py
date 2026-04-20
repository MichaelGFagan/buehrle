#!/usr/bin/env python3
"""
Install Chadwick Bureau tools (cwevent, cwgame, etc.) via Homebrew,
suppressing the auto-update step to reduce overhead.
"""
import argparse
import os
import shutil
import subprocess
import sys


def install(force: bool = False) -> None:
    existing = shutil.which('cwevent')
    if existing and not force:
        print(f'cwevent already installed at {existing}. Use --force to reinstall.')
        return

    if not shutil.which('brew'):
        sys.exit('Homebrew not found. Install it from https://brew.sh, then re-run this script.')

    env = {**os.environ, 'HOMEBREW_NO_AUTO_UPDATE': '1'}
    subprocess.run(['brew', 'install', 'chadwick'], env=env, check=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Install Chadwick tools via Homebrew.')
    parser.add_argument('--force', action='store_true', help='Reinstall even if cwevent is already on PATH.')
    args = parser.parse_args()
    install(force=args.force)
