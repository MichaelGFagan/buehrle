import argparse
import datetime
import logging
import os
import time
import dlt
import polars as pl
import pyarrow as pa
import requests

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from typing import Iterator

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/buehrle.duckdb')
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'
REQUEST_TIMEOUT = 60
SLEEP_BETWEEN = 1

TODAY = datetime.date.today()
STATCAST_START_YEAR = 2015

RUNNING_SPLIT_TYPES = ('raw', 'percent')


def _fetch_csv(url: str) -> pl.DataFrame | None:
    response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    df = pl.read_csv(response.content, infer_schema=False)
    return df if df.height > 0 else None


def _inject_labels(df: pl.DataFrame, labels: dict) -> pl.DataFrame:
    existing = set(df.columns)
    additions = [pl.lit(str(v)).alias(k) for k, v in labels.items() if k not in existing]
    return df.with_columns(additions) if additions else df


def _to_arrow(df: pl.DataFrame, primary_keys: set[str]) -> pa.Table:
    table = df.to_arrow()
    schema = pa.schema([
        f.with_type(pa.utf8()).with_nullable(f.name not in primary_keys)
        if f.type == pa.large_utf8()
        else f
        for f in table.schema
    ])
    return table.cast(schema)


def _run_years(name: str, pks: set[str], start_year: int, end_year: int, iter_fn, update: bool = False) -> Iterator:
    state = dlt.current.resource_state()
    from_year = start_year if update else state.get('last_year', start_year)
    for year in range(from_year, end_year + 1):
        for labels, url in iter_fn(year):
            logging.info(f'Fetching {name} {year} {labels if labels else ""}')
            df = _fetch_csv(url)
            if df is None:
                continue
            df = _inject_labels(df, {'year': year, **labels})
            yield _to_arrow(df, pks)
            time.sleep(SLEEP_BETWEEN)
        state['last_year'] = year


@dlt.resource(name='sprint_speed', write_disposition='merge', primary_key=['player_id', 'year'])
def sprint_speed(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'sprint_speed', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'https://baseballsavant.mlb.com/leaderboard/sprint_speed?year={y}&position=&team=&min=0&csv=true')],
        update,
    )


# running_splits returns the same columns for raw vs percent but different values;
# split_type discriminates the row sets in a single table.
@dlt.resource(name='running_splits', write_disposition='merge', primary_key=['player_id', 'year', 'split_type'])
def running_splits(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for st in RUNNING_SPLIT_TYPES:
            yield ({'split_type': st},
                   f'https://baseballsavant.mlb.com/running_splits?type={st}&bats=&year={y}&position=&team=&min=5&csv=true')
    yield from _run_years('running_splits', {'player_id', 'year', 'split_type'}, start_year, end_year, iter_year, update)


@dlt.source
def statcast_running_leaderboards(start_year: int, end_year: int, update: bool = False):
    yield sprint_speed(start_year, end_year, update)
    yield running_splits(start_year, end_year, update)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=STATCAST_START_YEAR)
    parser.add_argument('--end', type=int, default=TODAY.year)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--resources', nargs='+', default=None)
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name='statcast_running_leaderboards',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='statcast_running_leaderboards',
    )

    source = statcast_running_leaderboards(start_year=args.start, end_year=args.end, update=args.update)
    if args.resources:
        source = source.with_resources(*args.resources)

    if args.full_refresh:
        with pipeline.destination_client() as client:
            try:
                client.drop_storage()
            except DatabaseUndefinedRelation:
                pass
        pipeline.drop()

    load_info = pipeline.run(source)
    print(load_info)
