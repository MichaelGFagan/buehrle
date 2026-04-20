import argparse
import datetime
import logging
import os
import dlt
import polars as pl
import pyarrow as pa

from calendar import monthrange
from dlt.sources.helpers import requests
from typing import Iterator

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

TODAY = datetime.date.today()
SEASON_START_MONTH = 3
DB_PATH = os.path.join(os.path.dirname(__file__), '../data/buehrle.duckdb')
BASE_STATCAST_URL = 'https://baseballsavant.mlb.com/statcast_search/csv'

COLUMN_RENAMES = {
    'pitcher.1':   'pitcher_1',
    'fielder_2.1': 'fielder_2_1',
}


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
    PRIMARY_KEYS = {'game_pk', 'at_bat_number', 'pitch_number'}
    table = df.to_arrow()
    schema = pa.schema([
        f.with_type(pa.utf8()).with_nullable(f.name not in PRIMARY_KEYS)
        if f.type == pa.large_utf8()
        else f
        for f in table.schema
    ])
    return table.cast(schema)


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


def _parse_date(s: str, end: bool = False) -> datetime.date:
    if len(s) == 4:
        year = int(s)
        return datetime.date(year, 12, 31) if end else datetime.date(year, SEASON_START_MONTH, 1)
    return datetime.date.fromisoformat(s)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default=str(TODAY.year))
    parser.add_argument('--end', default=str(TODAY.year))
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name='statcast',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='statcast',
    )

    source = statcast_source(
        start_date=_parse_date(args.start),
        end_date=_parse_date(args.end, end=True),
        update=args.update,
    )

    if args.full_refresh:
        with pipeline.destination_client() as client:
            client.drop_storage()
        pipeline.drop()

    load_info = pipeline.run(source)
    print(load_info)
