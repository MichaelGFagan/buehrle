import logging
import os
import dlt
import polars as pl
import pyarrow as pa

from typing import Iterator

from loaders.cli import add_season_args, resolve_seasons, run_loader, validate_season_args
from loaders.retrosheet.retrosheet_sync import REPO_DIR, check
from loaders.dlt_utils import make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

EARLIEST_SEASON = 1871
COLUMN_RENAMES = {'ID': 'umpire_id', 'last': 'last_name', 'first': 'first_name'}
PRIMARY_KEYS = {'umpire_id', 'season'}


def _load_season(season: int) -> pa.Table:
    path = os.path.join(REPO_DIR, f'seasons/{season}/UMPIRES{season}.txt')
    df = (
        pl.read_csv(path, infer_schema_length=0)
        .rename(COLUMN_RENAMES)
        .with_columns(pl.lit(str(season)).alias('season'))
    )
    return to_arrow(df, PRIMARY_KEYS)


@dlt.resource(
    name='umpires',
    write_disposition='merge',
    primary_key=['umpire_id', 'season'],
)
def umpires(start_season: int, end_season: int, update: bool = False) -> Iterator[pa.Table]:
    state = dlt.current.resource_state()
    from_season = start_season if update else state.get('last_season', start_season)

    for season in range(from_season, end_season + 1):
        path = os.path.join(REPO_DIR, f'seasons/{season}/UMPIRES{season}.txt')
        if not os.path.exists(path):
            logging.warning(f'No umpires file for {season}')
            continue

        logging.info(f'Loading umpires for {season}')
        yield _load_season(season)
        state['last_season'] = season


@dlt.source
def retrosheet_umpires(start_season: int, end_season: int, update: bool = False):
    yield umpires(start_season, end_season, update)


def register(subparsers):
    parser = subparsers.add_parser('retrosheet-umpires', help='Retrosheet umpires')
    add_season_args(parser, EARLIEST_SEASON)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    check()
    validate_season_args(parser, args)
    start_season, end_season = resolve_seasons(args, EARLIEST_SEASON)

    pipeline = make_pipeline('retrosheet_umpires')

    source = retrosheet_umpires(
        start_season=start_season,
        end_season=end_season,
        update=args.update,
    )

    run_loader(pipeline, source, args)
