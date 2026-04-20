import argparse
import logging
import os
import sys
import dlt
import polars as pl
import pyarrow as pa

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from enum import Enum
from typing import Iterator

sys.path.insert(0, os.path.dirname(__file__))
from retrosheet_sync import REPO_DIR, check

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/buehrle.duckdb')

SEASON_PATH = os.path.join(REPO_DIR, 'seasons/{season}/GL{season}.TXT')
PLAYOFF_PATH = os.path.join(REPO_DIR, 'gamelog/GL{suffix}.TXT')

COLUMNS = [
    'date', 'game_num', 'day_of_week', 'visiting_team',
    'visiting_team_league', 'visiting_team_game_num', 'home_team',
    'home_team_league', 'home_team_game_num', 'visiting_score',
    'home_score', 'num_outs', 'day_night', 'completion_info',
    'forfeit_info', 'protest_info', 'park_id', 'attendance',
    'time_of_game_minutes', 'visiting_line_score',
    'home_line_score', 'visiting_abs', 'visiting_hits',
    'visiting_doubles', 'visiting_triples', 'visiting_homeruns',
    'visiting_rbi', 'visiting_sac_hits', 'visiting_sac_flies',
    'visiting_hbp', 'visiting_bb', 'visiting_iw', 'visiting_k',
    'visiting_sb', 'visiting_cs', 'visiting_gdp', 'visiting_ci',
    'visiting_lob', 'visiting_pitchers_used',
    'visiting_individual_er', 'visiting_er', 'visiting_wp',
    'visiting_balks', 'visiting_po', 'visiting_assists',
    'visiting_errors', 'visiting_pb', 'visiting_dp',
    'visiting_tp', 'home_abs', 'home_hits', 'home_doubles',
    'home_triples', 'home_homeruns', 'home_rbi',
    'home_sac_hits', 'home_sac_flies', 'home_hbp', 'home_bb',
    'home_iw', 'home_k', 'home_sb', 'home_cs', 'home_gdp',
    'home_ci', 'home_lob', 'home_pitchers_used',
    'home_individual_er', 'home_er', 'home_wp', 'home_balks',
    'home_po', 'home_assists', 'home_errors', 'home_pb',
    'home_dp', 'home_tp', 'ump_home_id', 'ump_home_name',
    'ump_first_id', 'ump_first_name', 'ump_second_id',
    'ump_second_name', 'ump_third_id', 'ump_third_name',
    'ump_lf_id', 'ump_lf_name', 'ump_rf_id', 'ump_rf_name',
    'visiting_manager_id', 'visiting_manager_name',
    'home_manager_id', 'home_manager_name',
    'winning_pitcher_id', 'winning_pitcher_name',
    'losing_pitcher_id', 'losing_pitcher_name',
    'save_pitcher_id', 'save_pitcher_name',
    'game_winning_rbi_id', 'game_winning_rbi_name',
    'visiting_starting_pitcher_id', 'visiting_starting_pitcher_name',
    'home_starting_pitcher_id', 'home_starting_pitcher_name',
    'visiting_1_id', 'visiting_1_name', 'visiting_1_pos',
    'visiting_2_id', 'visiting_2_name', 'visiting_2_pos',
    'visiting_3_id', 'visiting_3_name', 'visiting_3_pos',  # pybaseball has a bug: 'visiting_2_id.1'
    'visiting_4_id', 'visiting_4_name', 'visiting_4_pos',
    'visiting_5_id', 'visiting_5_name', 'visiting_5_pos',
    'visiting_6_id', 'visiting_6_name', 'visiting_6_pos',
    'visiting_7_id', 'visiting_7_name', 'visiting_7_pos',
    'visiting_8_id', 'visiting_8_name', 'visiting_8_pos',
    'visiting_9_id', 'visiting_9_name', 'visiting_9_pos',
    'home_1_id', 'home_1_name', 'home_1_pos',
    'home_2_id', 'home_2_name', 'home_2_pos',
    'home_3_id', 'home_3_name', 'home_3_pos',
    'home_4_id', 'home_4_name', 'home_4_pos',
    'home_5_id', 'home_5_name', 'home_5_pos',
    'home_6_id', 'home_6_name', 'home_6_pos',
    'home_7_id', 'home_7_name', 'home_7_pos',
    'home_8_id', 'home_8_name', 'home_8_pos',
    'home_9_id', 'home_9_name', 'home_9_pos',
    'misc', 'acquisition_info',
]


class RetrosheetPlayoff(Enum):
    WORLD_SERIES = 'WS'
    ALL_STAR = 'AS'
    WILD_CARD = 'WC'
    DIVISION_SERIES = 'DV'
    LEAGUE_CHAMPIONSHIP_SERIES = 'LC'

    def game_type(self) -> str:
        return self.name.lower()


PRIMARY_KEYS = {'home_team', 'date', 'game_num'}


def _add_game_type(table: pa.Table, game_type: str) -> pa.Table:
    return table.append_column('game_type', pa.array([game_type] * len(table), type=pa.utf8()))


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
    name='game_logs',
    write_disposition='merge',
    primary_key=['home_team', 'date', 'game_num'],
)
def game_logs(
    start_season: int,
    end_season: int,
    playoffs: str = 'include',
    update: bool = False,
) -> Iterator[dict]:
    state = dlt.current.resource_state()
    from_season = start_season if update else state.get('last_season', start_season)

    if playoffs != 'only':
        for season in range(from_season, end_season + 1):
            path = SEASON_PATH.format(season=season)
            for candidate in [path, path.lower()]:
                try:
                    df = _fetch(candidate)
                    break
                except Exception:
                    continue
            else:
                logging.warning(f'Could not load season {season}')
                continue
            yield _add_game_type(df, 'regular_season')
            state['last_season'] = season

    if playoffs in ('include', 'only'):
        for playoff in RetrosheetPlayoff:
            try:
                df = _fetch(PLAYOFF_PATH.format(suffix=playoff.value))
                yield _add_game_type(df, playoff.game_type())
            except Exception as e:
                logging.warning(f'Could not load {playoff.name}: {e}')


@dlt.source
def retrosheet(start_season: int, end_season: int, playoffs: str = 'include', update: bool = False):
    yield game_logs(start_season, end_season, playoffs, update)


if __name__ == '__main__':
    check()
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=1871)
    parser.add_argument('--end', type=int, default=2025)
    parser.add_argument('--playoffs', choices=['include', 'only'], default='include')
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name='retrosheet',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='retrosheet',
    )

    source = retrosheet(
        start_season=args.start,
        end_season=args.end,
        playoffs=args.playoffs,
        update=args.update,
    )

    if args.full_refresh:
        with pipeline.destination_client() as client:
            try:
                client.drop_storage()
            except DatabaseUndefinedRelation:
                pass
        pipeline.drop()

    load_info = pipeline.run(source)
    print(load_info)
