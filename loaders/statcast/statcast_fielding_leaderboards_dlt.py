import argparse
import datetime
import json
import logging
import os
import re
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
STATCAST_START_YEAR = 2016  # OAA leaderboards begin 2016

FIELDING_POSITIONS = ('2', '3', '4', '5', '6', '7', '8', '9', 'OF')

EMBEDDED_JSON_RE = re.compile(r'(?:const|var|let)\s+data\s*=\s*(\[[\s\S]*?\]);')


def _fetch_csv(url: str) -> pl.DataFrame | None:
    response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    df = pl.read_csv(response.content, infer_schema=False)
    return df if df.height > 0 else None


def _fetch_embedded_json(url: str) -> pl.DataFrame | None:
    response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    match = EMBEDDED_JSON_RE.search(response.text)
    if not match:
        return None
    rows = json.loads(match.group(1))
    if not rows:
        return None
    # Coerce everything to string so arrow cast logic below stays consistent with CSV-based resources
    rows = [{k: (str(v) if v is not None else None) for k, v in row.items()} for row in rows]
    columns = list({k for row in rows for k in row})
    schema = {col: pl.String for col in columns}
    return pl.DataFrame(rows, schema=schema)


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
        for labels, fetcher in iter_fn(year):
            logging.info(f'Fetching {name} {year} {labels if labels else ""}')
            df = fetcher()
            if df is None:
                continue
            df = _inject_labels(df, {'year': year, **labels})
            yield _to_arrow(df, pks)
            time.sleep(SLEEP_BETWEEN)
        state['last_year'] = year


@dlt.resource(name='outs_above_average', write_disposition='merge', primary_key=['player_id', 'year', 'position'])
def outs_above_average(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for pos in FIELDING_POSITIONS:
            if pos == 'OF':
                continue
            url = (
                'https://baseballsavant.mlb.com/leaderboard/outs_above_average'
                f'?type=Fielder&startYear={y}&endYear={y}&split=no&team=&range=year&min=10&pos={pos}&roles=&viz=hide&csv=true'
            )
            yield ({'position': pos}, lambda u=url: _fetch_csv(u))
    yield from _run_years('outs_above_average', {'player_id', 'year', 'position'}, start_year, end_year, iter_year, update)


# fielding-run-value and catcher_framing come from the embedded page JSON, where the player id column is 'id'.
@dlt.resource(name='fielding_run_value', write_disposition='merge', primary_key=['id', 'year', 'position'])
def fielding_run_value(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for pos in FIELDING_POSITIONS:
            url = f'https://baseballsavant.mlb.com/leaderboard/fielding-run-value?year={y}&minInnings=0.1&minResults=0.1&pos={pos}&roles=&viz=show&csv=true'
            yield ({'position': pos}, lambda u=url: _fetch_embedded_json(u))
    yield from _run_years('fielding_run_value', {'id', 'year', 'position'}, start_year, end_year, iter_year, update)


@dlt.resource(name='outfield_directional_oaa', write_disposition='merge', primary_key=['player_id', 'year'])
def outfield_directional_oaa(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'outfield_directional_oaa', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, lambda u=f'https://baseballsavant.mlb.com/directional_outs_above_average?year={y}&min=0&team=&csv=true': _fetch_csv(u))],
        update,
    )


@dlt.resource(name='outfield_catch_prob', write_disposition='merge', primary_key=['player_id', 'year'])
def outfield_catch_prob(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'outfield_catch_prob', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, lambda u=f'https://baseballsavant.mlb.com/leaderboard/catch_probability?type=player&min=0&year={y}&total=&csv=true': _fetch_csv(u))],
        update,
    )


@dlt.resource(name='outfielder_jump', write_disposition='merge', primary_key=['resp_fielder_id', 'year'])
def outfielder_jump(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'outfielder_jump', {'resp_fielder_id', 'year'}, start_year, end_year,
        lambda y: [({}, lambda u=f'https://baseballsavant.mlb.com/leaderboard/outfield_jump?year={y}&min=10&csv=true': _fetch_csv(u))],
        update,
    )


@dlt.resource(name='catcher_poptime', write_disposition='merge', primary_key=['entity_id', 'year'])
def catcher_poptime(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'catcher_poptime', {'entity_id', 'year'}, start_year, end_year,
        lambda y: [({}, lambda u=f'https://baseballsavant.mlb.com/leaderboard/poptime?year={y}&team=&min2b=1&min3b=0&csv=true': _fetch_csv(u))],
        update,
    )


@dlt.resource(name='catcher_framing', write_disposition='merge', primary_key=['id', 'year'])
def catcher_framing(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'catcher_framing', {'id', 'year'}, start_year, end_year,
        lambda y: [({}, lambda u=f'https://baseballsavant.mlb.com/catcher_framing?year={y}&team=&min=0&sort=4,1&csv=true': _fetch_embedded_json(u))],
        update,
    )


@dlt.source
def statcast_fielding_leaderboards(start_year: int, end_year: int, update: bool = False):
    yield outs_above_average(start_year, end_year, update)
    yield fielding_run_value(start_year, end_year, update)
    yield outfield_directional_oaa(start_year, end_year, update)
    yield outfield_catch_prob(start_year, end_year, update)
    yield outfielder_jump(start_year, end_year, update)
    yield catcher_poptime(start_year, end_year, update)
    yield catcher_framing(start_year, end_year, update)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=STATCAST_START_YEAR)
    parser.add_argument('--end', type=int, default=TODAY.year)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--resources', nargs='+', default=None)
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name='statcast_fielding_leaderboards',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='statcast_fielding_leaderboards',
    )

    source = statcast_fielding_leaderboards(start_year=args.start, end_year=args.end, update=args.update)
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
