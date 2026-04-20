import argparse
import logging
import os
import sys
import dlt
import polars as pl
import pyarrow as pa

from typing import Iterator

sys.path.insert(0, os.path.dirname(__file__))
from retrosheet_sync import REPO_DIR, sync

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/buehrle.duckdb')

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
    table = pl.read_csv(path, has_header=False, new_columns=COLUMNS, infer_schema_length=0).to_arrow()
    schema = pa.schema([
        f.with_type(pa.utf8()).with_nullable(f.name not in PRIMARY_KEYS)
        if f.type == pa.large_utf8()
        else f
        for f in table.schema
    ])
    return table.cast(schema)


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


if __name__ == '__main__':
    sync()
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=1877)
    parser.add_argument('--end', type=int, default=2025)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name='retrosheet_schedules',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='retrosheet_schedules',
    )

    source = retrosheet_schedules(
        start_season=args.start,
        end_season=args.end,
        update=args.update,
    )

    if args.full_refresh:
        with pipeline.destination_client() as client:
            client.drop_storage()
        pipeline.drop()

    load_info = pipeline.run(source)
    print(load_info)
