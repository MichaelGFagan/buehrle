"""
Clones or updates the Chadwick Bureau retrosheet repo into data/retrosheet.
Run this before any retrosheet loaders that read from the local clone.
"""
import logging
import os
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

REPO_URL = 'https://github.com/chadwickbureau/retrosheet.git'
REPO_DIR = os.path.join(os.path.dirname(__file__), '../../data/retrosheet')


def check() -> None:
    if not os.path.isdir(os.path.join(REPO_DIR, '.git')):
        sys.exit(
            f'Retrosheet repo not found at {REPO_DIR}.\n'
            'Run: python loaders/retrosheet/retrosheet_sync.py'
        )


def sync():
    if os.path.isdir(os.path.join(REPO_DIR, '.git')):
        logging.info(f'Repo already cloned at {REPO_DIR}, pulling latest...')
        subprocess.run(['git', '-C', REPO_DIR, 'pull', 'origin', 'master'], check=True)
    else:
        logging.info(f'Cloning retrosheet repo into {REPO_DIR}...')
        subprocess.run(
            ['git', 'clone', '--branch', 'master', '--single-branch', REPO_URL, REPO_DIR],
            check=True,
        )
    logging.info('Done.')


if __name__ == '__main__':
    sync()
