import logging
import os
import dlt
import polars as pl
import pyarrow as pa

from typing import Iterator

from loaders.cli import add_season_args, resolve_seasons, validate_season_args
from loaders.retrosheet.retrosheet_sync import REPO_DIR, check
from loaders.dlt_utils import handle_full_refresh, make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

EARLIEST_SEASON = 1871
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


def register(subparsers):
    parser = subparsers.add_parser('retrosheet-rosters', help='Retrosheet rosters')
    add_season_args(parser, EARLIEST_SEASON)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    check()
    validate_season_args(parser, args)
    start_season, end_season = resolve_seasons(args, EARLIEST_SEASON)

    pipeline = make_pipeline('retrosheet_rosters')

    source = retrosheet_rosters(
        start_season=start_season,
        end_season=end_season,
        update=args.update,
    )

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
