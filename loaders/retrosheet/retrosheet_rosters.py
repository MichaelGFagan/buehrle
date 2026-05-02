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

COLUMNS = ['player_id', 'last_name', 'first_name', 'bats', 'throws', 'team', 'position']
PRIMARY_KEYS = {'player_id', 'team', 'season'}


def _load_season(season: int) -> pa.Table:
    season_dir = os.path.join(REPO_DIR, f'seasons/{season}')
    roster_files = [f for f in os.listdir(season_dir) if f.endswith('.ROS')]

    tables = []
    for filename in roster_files:
        path = os.path.join(season_dir, filename)
        table = pl.read_csv(path, has_header=False, new_columns=COLUMNS, infer_schema_length=0)
        tables.append(table)

    combined = pl.concat(tables).with_columns(pl.lit(str(season)).alias('season'))
    return to_arrow(combined, PRIMARY_KEYS)


@dlt.resource(
    name='rosters',
    write_disposition='merge',
    primary_key=['player_id', 'team', 'season'],
)
def rosters(start_season: int, end_season: int, update: bool = False) -> Iterator[pa.Table]:
    state = dlt.current.resource_state()
    from_season = start_season if update else state.get('last_season', start_season)

    for season in range(from_season, end_season + 1):
        season_dir = os.path.join(REPO_DIR, f'seasons/{season}')
        if not os.path.isdir(season_dir):
            logging.warning(f'No season directory for {season}')
            continue
        if not any(f.endswith('.ROS') for f in os.listdir(season_dir)):
            logging.warning(f'No roster files for {season}')
            continue

        logging.info(f'Loading rosters for {season}')
        yield _load_season(season)
        state['last_season'] = season


@dlt.source
def retrosheet_rosters(start_season: int, end_season: int, update: bool = False):
    yield rosters(start_season, end_season, update)


if __name__ == '__main__':
    check()
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=1871)
    parser.add_argument('--end', type=int, default=2025)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    args = parser.parse_args()

    pipeline = make_pipeline('retrosheet_rosters')

    source = retrosheet_rosters(
        start_season=args.start,
        end_season=args.end,
        update=args.update,
    )

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
