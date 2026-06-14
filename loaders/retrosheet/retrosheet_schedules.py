import logging
import os
import dlt
import polars as pl
import pyarrow as pa

from typing import Iterator

from loaders.cli import add_season_args, resolve_seasons, run_loader, validate_season_args
from loaders.retrosheet.retrosheet_sync import REPO_DIR, sync
from loaders.dlt_utils import make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

EARLIEST_SEASON = 1877
SCHEDULE_PATH = os.path.join(REPO_DIR, 'seasons/{season}/{season}schedule.csv')

# Canonical (modern) Retrosheet schedule schema. Files come in two shapes:
# older seasons omit `location` (12 columns); 2024+ include it (13 columns).
# We normalise both to this 13-column schema, null-filling `location` when absent.
COLUMNS = [
    'date', 'game_num', 'day_of_week', 'visiting_team', 'visiting_team_league',
    'visiting_team_game_num', 'home_team', 'home_team_league',
    'home_team_game_num', 'day_night', 'location',
    'postponement_cancellation', 'date_of_makeup',
]
LOCATION_INDEX = COLUMNS.index('location')

PRIMARY_KEYS = {'date', 'game_num', 'home_team'}

PIPELINE_NAME = 'retrosheet_schedules'  # destination schema (== dlt pipeline/dataset name)
# Status-grid watermark: {table: SQL expression yielding its time dimension}.
WATERMARKS = {'schedules': 'date'}


def _fetch(path: str) -> pa.Table:
    logging.info(f'Reading {path}')
    # Most season files carry a `Date,Num,Day,...` header row; reading with
    # has_header=False ingests it as data (leaking a literal 'Date' into the
    # date column), and duplicate header names (League/Game) rule out
    # has_header=True. So read headerless and strip the header row by content.
    df = pl.read_csv(path, has_header=False, infer_schema_length=0)
    df = df.filter(pl.col(df.columns[0]) != 'Date')
    if df.width == len(COLUMNS) - 1:  # older format lacks `location`
        df = df.insert_column(LOCATION_INDEX, pl.lit(None, dtype=pl.Utf8).alias('location'))
    df = df.rename({old: new for old, new in zip(df.columns, COLUMNS)})
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

    pipeline = make_pipeline(PIPELINE_NAME)

    source = retrosheet_schedules(
        start_season=start_season,
        end_season=end_season,
        update=args.update,
    )

    run_loader(pipeline, source, args)
