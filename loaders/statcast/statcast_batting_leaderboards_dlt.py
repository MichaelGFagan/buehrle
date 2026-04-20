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


@dlt.resource(name='exit_velo_barrels', write_disposition='merge', primary_key=['player_id', 'year'])
def exit_velo_barrels(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'exit_velo_barrels', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'https://baseballsavant.mlb.com/leaderboard/statcast?type=batter&year={y}&position=&team=&min=1&csv=true')],
        update,
    )


@dlt.resource(name='expected_stats', write_disposition='merge', primary_key=['player_id', 'year'])
def expected_stats(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'expected_stats', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=batter&year={y}&position=&team=&min=1&csv=true')],
        update,
    )


@dlt.resource(name='percentile_ranks', write_disposition='merge', primary_key=['player_id', 'year'])
def percentile_ranks(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'percentile_ranks', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'https://baseballsavant.mlb.com/leaderboard/percentile-rankings?type=batter&year={y}&position=&team=&csv=true')],
        update,
    )


@dlt.resource(name='pitch_arsenal_stats', write_disposition='merge', primary_key=['player_id', 'year', 'pitch_type'])
def pitch_arsenal_stats(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'pitch_arsenal_stats', {'player_id', 'year', 'pitch_type'}, start_year, end_year,
        lambda y: [({}, f'https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats?type=batter&pitchType=&year={y}&team=&min=1&csv=true')],
        update,
    )


# bat_tracking returns column 'id' (not 'player_id'); Savant began publishing this in 2023.
# game_type accepts 'Regular', 'Playoff', or 'all' (empty Savant filter — all game types).
@dlt.resource(name='bat_tracking', write_disposition='merge', primary_key=['id', 'year'])
def bat_tracking(start_year: int, end_year: int, update: bool = False, game_type: str = 'Regular') -> Iterator:
    gt = '' if game_type == 'all' else game_type
    yield from _run_years(
        'bat_tracking', {'id', 'year'}, start_year, end_year,
        lambda y: [({}, (
            'https://baseballsavant.mlb.com/leaderboard/bat-tracking?'
            f'gameType={gt}&minSwings=1&minGroupSwings=1&seasonStart={y}&seasonEnd={y}&type=batter&csv=true'
        ))],
        update,
    )


@dlt.source
def statcast_batting_leaderboards(start_year: int, end_year: int, update: bool = False, game_type: str = 'Regular'):
    yield exit_velo_barrels(start_year, end_year, update)
    yield expected_stats(start_year, end_year, update)
    yield percentile_ranks(start_year, end_year, update)
    yield pitch_arsenal_stats(start_year, end_year, update)
    yield bat_tracking(start_year, end_year, update, game_type)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=STATCAST_START_YEAR)
    parser.add_argument('--end', type=int, default=TODAY.year)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--resources', nargs='+', default=None)
    parser.add_argument('--game-type', choices=['Regular', 'Playoff', 'all'], default='Regular',
                        help="Savant gameType filter for bat_tracking (default 'Regular')")
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name='statcast_batting_leaderboards',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='statcast_batting_leaderboards',
    )

    source = statcast_batting_leaderboards(start_year=args.start, end_year=args.end, update=args.update, game_type=args.game_type)
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
