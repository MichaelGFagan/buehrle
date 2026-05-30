import datetime
import logging
import dlt
import polars as pl

from calendar import monthrange
from dlt.sources.helpers import requests
from typing import Iterator

from loaders.cli import add_date_args, add_season_args, resolve_dates, validate_scope_args
from loaders.dlt_utils import handle_full_refresh, make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

TODAY = datetime.date.today()
SEASON_START_MONTH = 3
SEASON_END_MONTH = 12
SEASON_END_DAY = 31
EARLIEST_SEASON = 2008  # Statcast era
BASE_STATCAST_URL = 'https://baseballsavant.mlb.com/statcast_search/csv'

COLUMN_RENAMES = {
    'pitcher.1':   'pitcher_1',
    'fielder_2.1': 'fielder_2_1',
}

PRIMARY_KEYS = {'game_pk', 'at_bat_number', 'pitch_number'}


def _fetch_range(start_date: datetime.date, end_date: datetime.date):
    params = {
        'all': 'true',
        'type': 'details',
        'game_date_gt': start_date.strftime('%Y-%m-%d'),
        'game_date_lt': end_date.strftime('%Y-%m-%d'),
        'hfGT': 'R|',
        'min_pitches': 0,
        'min_results': 0,
        'group_by': 'name',
        'sort_col': 'pitches',
        'sort_order': 'desc',
        'min_abs': 0,
    }

    logging.info(f'Fetching statcast {start_date} to {end_date}')
    response = requests.get(BASE_STATCAST_URL, params=params)
    response.raise_for_status()

    df = pl.read_csv(response.content, infer_schema=False)
    df = df.rename({k: v for k, v in COLUMN_RENAMES.items() if k in df.columns})
    return to_arrow(df, PRIMARY_KEYS)


@dlt.resource(
    name='pitches',
    write_disposition='merge',
    primary_key=['game_pk', 'at_bat_number', 'pitch_number'],
)
def pitches(start_date: datetime.date, end_date: datetime.date, update: bool = False) -> Iterator:
    state = dlt.current.resource_state()
    if update or 'last_date' not in state:
        from_date = start_date
    else:
        from_date = datetime.date.fromisoformat(state['last_date'])

    year, month = from_date.year, from_date.month
    while datetime.date(year, month, 1) <= end_date:
        month_start = max(datetime.date(year, month, 1), from_date)
        month_end = min(datetime.date(year, month, monthrange(year, month)[1]), end_date)

        yield _fetch_range(month_start, month_end)
        state['last_date'] = month_end.isoformat()

        month += 1
        if month > 12:
            year, month = year + 1, 1


@dlt.source
def statcast_source(start_date: datetime.date, end_date: datetime.date, update: bool = False):
    yield pitches(start_date, end_date, update)


def _season_bounds(year: int) -> tuple[datetime.date, datetime.date]:
    return datetime.date(year, SEASON_START_MONTH, 1), datetime.date(year, SEASON_END_MONTH, SEASON_END_DAY)


def register(subparsers):
    parser = subparsers.add_parser('statcast-pitches', help='Statcast pitch-level data')
    add_season_args(parser, EARLIEST_SEASON)
    add_date_args(parser)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    validate_scope_args(parser, args)
    start_date, end_date = resolve_dates(args, EARLIEST_SEASON, _season_bounds)

    pipeline = make_pipeline('statcast_pitches')

    source = statcast_source(
        start_date=start_date,
        end_date=end_date,
        update=args.update,
    )

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
