import logging
import os
import dlt
import polars as pl
import pyarrow as pa

from typing import Iterator

from loaders.cli import add_season_args, resolve_seasons, validate_season_args
from loaders.retrosheet.retrosheet_sync import REPO_DIR, sync
from loaders.dlt_utils import handle_full_refresh, make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

EARLIEST_SEASON = 1877
SCHEDULE_PATH = os.path.join(REPO_DIR, 'seasons/{season}/{season}schedule.csv')

COLUMNS = [
    'date', 'game_num', 'day_of_week', 'visiting_team', 'visiting_team_league',
    'visiting_team_game_num', 'home_team', 'home_team_league',
    'home_team_game_num', 'day_night', 'postponement_cancellation',
    'date_of_makeup',
]

PRIMARY_KEYS = {'date', 'game_num', 'home_team'}


def _fetch(path: str) -> pa.Table:
    logging.info(f'Reading {path}')
    df = pl.read_csv(path, has_header=False, new_columns=COLUMNS, infer_schema_length=0)
    return to_arrow(df, PRIMARY_KEYS)


@dlt.resource(
    name='schedules',
    write_disposition='merge',
    primary_key=['date', 'game_num', 'home_team'],
)
def schedules(start_season: int, end_season: int, update: bool = False) -> Iterator[pa.Table]:
    state = dlt.current.resource_state()
    from_season = start_season if update else state.get('last_season', start_season)

    for season in range(from_season, end_season + 1):
        path = SCHEDULE_PATH.format(season=season)
        for candidate in [path, path.lower()]:
            try:
                table = _fetch(candidate)
                break
            except Exception:
                continue
        else:
            logging.warning(f'Could not load schedule for {season}')
            continue
        yield table
        state['last_season'] = season


@dlt.source
def retrosheet_schedules(start_season: int, end_season: int, update: bool = False):
    yield schedules(start_season, end_season, update)


def register(subparsers):
    parser = subparsers.add_parser('retrosheet-schedules', help='Retrosheet schedules')
    add_season_args(parser, EARLIEST_SEASON)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    sync()
    validate_season_args(parser, args)
    start_season, end_season = resolve_seasons(args, EARLIEST_SEASON)

    pipeline = make_pipeline('retrosheet_schedules')

    source = retrosheet_schedules(
        start_season=start_season,
        end_season=end_season,
        update=args.update,
    )

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
