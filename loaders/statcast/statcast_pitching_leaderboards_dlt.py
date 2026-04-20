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

ARSENAL_TYPES = ('avg_speed', 'n', 'avg_spin', 'avg_break_x', 'avg_break_z', 'avg_vert_break')
PITCH_TYPES = ('FF', 'SI', 'FC', 'SL', 'CH', 'CU', 'FS', 'KN', 'ST', 'SV')
ACTIVE_SPIN_TYPES = ('spin-based', 'movement-based')


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
        lambda y: [({}, f'https://baseballsavant.mlb.com/leaderboard/statcast?type=pitcher&year={y}&position=&team=&min=1&csv=true')],
        update,
    )


@dlt.resource(name='expected_stats', write_disposition='merge', primary_key=['player_id', 'year'])
def expected_stats(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'expected_stats', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=pitcher&year={y}&position=&team=&min=1&csv=true')],
        update,
    )


@dlt.resource(name='percentile_ranks', write_disposition='merge', primary_key=['player_id', 'year'])
def percentile_ranks(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'percentile_ranks', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'https://baseballsavant.mlb.com/leaderboard/percentile-rankings?type=pitcher&year={y}&position=&team=&csv=true')],
        update,
    )


# pitch_arsenals columns pivot based on arsenal_type (avg_speed / avg_spin / avg_break_x / etc.)
# so each type is stored as a separate arsenal_type-labeled row set sharing (pitcher, year).
@dlt.resource(name='pitch_arsenals', write_disposition='merge', primary_key=['pitcher', 'year', 'arsenal_type'])
def pitch_arsenals(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for at in ARSENAL_TYPES:
            yield ({'arsenal_type': at},
                   f'https://baseballsavant.mlb.com/leaderboard/pitch-arsenals?year={y}&min=100&type={at}&hand=&csv=true')
    yield from _run_years('pitch_arsenals', {'pitcher', 'year', 'arsenal_type'}, start_year, end_year, iter_year, update)


@dlt.resource(name='pitch_arsenal_stats', write_disposition='merge', primary_key=['player_id', 'year', 'pitch_type'])
def pitch_arsenal_stats(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from _run_years(
        'pitch_arsenal_stats', {'player_id', 'year', 'pitch_type'}, start_year, end_year,
        lambda y: [({}, f'https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats?type=pitcher&pitchType=&year={y}&team=&min=1&minPitches=1&csv=true')],
        update,
    )


@dlt.resource(name='pitch_movement', write_disposition='merge', primary_key=['pitcher_id', 'year', 'pitch_type'])
def pitch_movement(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for pt in PITCH_TYPES:
            yield ({}, (
                f'https://baseballsavant.mlb.com/leaderboard/pitch-movement?year={y}&team=&min=50'
                f'&pitch_type={pt}&hand=&x=pitcher_break_x_hidden&z=pitcher_break_z_hidden&csv=true'
            ))
    yield from _run_years('pitch_movement', {'pitcher_id', 'year', 'pitch_type'}, start_year, end_year, iter_year, update)


# active_spin columns pivot by spin_type (spin-based vs movement-based); label rows so both can coexist.
@dlt.resource(name='active_spin', write_disposition='merge', primary_key=['entity_id', 'year', 'spin_type'])
def active_spin(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for st in ACTIVE_SPIN_TYPES:
            yield ({'spin_type': st},
                   f'https://baseballsavant.mlb.com/leaderboard/active-spin?year={y}_{st}&min=50&hand=&csv=true')
    yield from _run_years('active_spin', {'entity_id', 'year', 'spin_type'}, start_year, end_year, iter_year, update)


# bat_tracking from pitcher's perspective; column 'id' holds pitcher id. Data begins 2023.
# game_type accepts 'Regular', 'Playoff', or 'all' (empty Savant filter — all game types).
@dlt.resource(name='bat_tracking', write_disposition='merge', primary_key=['id', 'year'])
def bat_tracking(start_year: int, end_year: int, update: bool = False, game_type: str = 'Regular') -> Iterator:
    gt = '' if game_type == 'all' else game_type
    yield from _run_years(
        'bat_tracking', {'id', 'year'}, start_year, end_year,
        lambda y: [({}, (
            'https://baseballsavant.mlb.com/leaderboard/bat-tracking?'
            f'gameType={gt}&minSwings=1&minGroupSwings=1&seasonStart={y}&seasonEnd={y}&type=pitcher&csv=true'
        ))],
        update,
    )


@dlt.source
def statcast_pitching_leaderboards(start_year: int, end_year: int, update: bool = False, game_type: str = 'Regular'):
    yield exit_velo_barrels(start_year, end_year, update)
    yield expected_stats(start_year, end_year, update)
    yield percentile_ranks(start_year, end_year, update)
    yield pitch_arsenals(start_year, end_year, update)
    yield pitch_arsenal_stats(start_year, end_year, update)
    yield pitch_movement(start_year, end_year, update)
    yield active_spin(start_year, end_year, update)
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
        pipeline_name='statcast_pitching_leaderboards',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='statcast_pitching_leaderboards',
    )

    source = statcast_pitching_leaderboards(start_year=args.start, end_year=args.end, update=args.update, game_type=args.game_type)
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
