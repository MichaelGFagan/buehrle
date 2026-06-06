import io
import logging
import os
import shutil
import subprocess
import sys

import dlt
import polars as pl
import pyarrow as pa

from typing import Iterator

from loaders.cli import add_season_args, resolve_seasons, run_loader, validate_season_args
from loaders.retrosheet.retrosheet_sync import REPO_DIR, check
from loaders.dlt_utils import make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

EARLIEST_SEASON = 1871
SEASONS_DIR = os.path.join(REPO_DIR, 'seasons')

FULL_EXTENSIONS = {'.EVA', '.EVN', '.EVE', '.EVR', '.EVF'}
BOX_EXTENSIONS  = {'.EBA', '.EBN', '.EBE', '.EBR', '.EBF'}
DEDUCED_EXTENSIONS = {'.EDA', '.EDN', '.EDF'}

PRIMARY_KEYS = {'GAME_ID', 'EVENT_ID'}

PIPELINE_NAME = 'retrosheet_events'  # destination schema (== dlt pipeline/dataset name)
# Status-grid watermark: {table: SQL expression yielding its time dimension}.
# These tables carry only game_id (e.g. ANA201804020); the season is its 4-char
# year slice (1-indexed chars 4-7, after the 3-char home-team code).
_SEASON_FROM_GAME_ID = 'substr(game_id, 4, 4)'
WATERMARKS = {
    'retrosheet_game_logs_full': _SEASON_FROM_GAME_ID,
    'retrosheet_game_logs_box': _SEASON_FROM_GAME_ID,
    'retrosheet_game_logs_deduced': _SEASON_FROM_GAME_ID,
}



def _event_files(season_dir: str, extensions: set[str]) -> list[str]:
    return [
        f for f in os.listdir(season_dir)
        if os.path.splitext(f)[1].upper() in extensions
    ]


def _run_cwevent(year: int, season_dir: str, filenames: list[str]) -> pa.Table | None:
    result = subprocess.run(
        ['cwevent', '-y', str(year), '-n', '-f', '0-63', '-x', '0-63', '-q'] + filenames,
        cwd=season_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    df = (
        pl.read_csv(io.StringIO(result.stdout), infer_schema_length=0)
        .with_columns(
            pl.int_range(pl.len()).over('GAME_ID').cast(pl.Utf8).alias('EVENT_ID')
        )
    )
    return to_arrow(df, PRIMARY_KEYS)


def _seasons(start: int, end: int) -> Iterator[tuple[int, str]]:
    for year in range(start, end + 1):
        season_dir = os.path.join(SEASONS_DIR, str(year))
        if os.path.isdir(season_dir):
            yield year, season_dir


def _resource(name: str, extensions: set[str]):
    @dlt.resource(
        name=name,
        write_disposition='merge',
        primary_key=['GAME_ID', 'EVENT_ID'],
    )
    def _load(start_season: int, end_season: int, update: bool = False) -> Iterator[pa.Table]:
        state = dlt.current.resource_state()
        from_season = start_season if update else state.get('last_season', start_season)

        for year, season_dir in _seasons(from_season, end_season):
            filenames = _event_files(season_dir, extensions)
            if not filenames:
                continue
            logging.info(f'[{name}] Processing {year} ({len(filenames)} files)')
            table = _run_cwevent(year, season_dir, filenames)
            if table is not None:
                yield table
            state['last_season'] = year

    return _load


game_logs_full     = _resource('retrosheet_game_logs_full',     FULL_EXTENSIONS)
game_logs_box      = _resource('retrosheet_game_logs_box',      BOX_EXTENSIONS)
game_logs_deduced  = _resource('retrosheet_game_logs_deduced',  DEDUCED_EXTENSIONS)


@dlt.source
def retrosheet_events(start_season: int, end_season: int, update: bool = False):
    yield game_logs_full(start_season, end_season, update)
    yield game_logs_box(start_season, end_season, update)
    yield game_logs_deduced(start_season, end_season, update)


def register(subparsers):
    parser = subparsers.add_parser('retrosheet-events', help='Retrosheet events (requires Chadwick cwevent)')
    add_season_args(parser, EARLIEST_SEASON)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    if not shutil.which('cwevent'):
        sys.exit(
            'cwevent not found. Install Chadwick by running:\n'
            '  buehrle install-chadwick'
        )
    check()
    validate_season_args(parser, args)
    start_season, end_season = resolve_seasons(args, EARLIEST_SEASON)

    pipeline = make_pipeline(PIPELINE_NAME)

    source = retrosheet_events(start_season=start_season, end_season=end_season, update=args.update)

    run_loader(pipeline, source, args)
