import argparse
import io
import logging
import os
import shutil
import subprocess
import sys

import dlt
import polars as pl
import pyarrow as pa

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from typing import Iterator

sys.path.insert(0, os.path.dirname(__file__))
from retrosheet_sync import REPO_DIR, check

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/buehrle.duckdb')
SEASONS_DIR = os.path.join(REPO_DIR, 'seasons')

FULL_EXTENSIONS = {'.EVA', '.EVN', '.EVE', '.EVR', '.EVF'}
BOX_EXTENSIONS  = {'.EBA', '.EBN', '.EBE', '.EBR', '.EBF'}
DEDUCED_EXTENSIONS = {'.EDA', '.EDN', '.EDF'}

PRIMARY_KEYS = {'GAME_ID', 'EVENT_ID'}



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
    table = df.to_arrow()
    schema = pa.schema([
        f.with_type(pa.utf8()).with_nullable(f.name not in PRIMARY_KEYS)
        if f.type == pa.large_utf8()
        else f
        for f in table.schema
    ])
    return table.cast(schema)


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


if __name__ == '__main__':
    if not shutil.which('cwevent'):
        sys.exit(
            'cwevent not found. Install Chadwick by running:\n'
            '  python loaders/retrosheet/install_chadwick.py'
        )
    check()

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--year', type=int)
    group.add_argument('--start', type=int, default=1871)
    parser.add_argument('--end', type=int, default=2025)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    args = parser.parse_args()

    start = args.year if args.year else args.start
    end   = args.year if args.year else args.end

    pipeline = dlt.pipeline(
        pipeline_name='retrosheet_events',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='retrosheet_events',
    )

    source = retrosheet_events(start_season=start, end_season=end, update=args.update)

    if args.full_refresh:
        with pipeline.destination_client() as client:
            try:
                client.drop_storage()
            except DatabaseUndefinedRelation:
                pass
        pipeline.drop()

    load_info = pipeline.run(source)
    print(load_info)
