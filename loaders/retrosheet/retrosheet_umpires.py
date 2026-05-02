import argparse
import logging
import os
import sys
import dlt
import polars as pl
import pyarrow as pa

from typing import Iterator

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from retrosheet_sync import REPO_DIR, check
from dlt_utils import handle_full_refresh, make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

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


if __name__ == '__main__':
    check()
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=1871)
    parser.add_argument('--end', type=int, default=2025)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    args = parser.parse_args()

    pipeline = make_pipeline('retrosheet_umpires')

    source = retrosheet_umpires(
        start_season=args.start,
        end_season=args.end,
        update=args.update,
    )

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
