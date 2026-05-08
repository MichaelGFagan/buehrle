"""
Lahman is distributed via SABR (https://sabr.org/lahman-database/), hosted on Box — no stable
scriptable URL. The user manually downloads the annual release into data/lahman/; this loader
replaces those CSVs into DuckDB.

Filenames are hardcoded (not globbed) so that a renamed file from SABR fails loudly, and so new
tables SABR adds surface as a warning rather than being silently picked up. Table names mirror
buehrle_dbt/models/sources/lahman/_source_lahman.yml so dbt sources can be repointed cleanly.
"""

import argparse
import logging
import os
from typing import Iterator

import dlt
import polars as pl
import pyarrow as pa

from loaders.dlt_utils import handle_full_refresh, make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

DEFAULT_DATA_DIR = os.path.join(os.path.dirname(__file__), '../../data/lahman')

# Column overrides for tables whose CSV headers contain identifiers DuckDB rejects unquoted
# (e.g. `2B`, `3B`). Same overrides as the existing dbt source.
BATTING_COLUMNS = [
    'playerID', 'yearID', 'stint', 'teamID', 'lgID',
    'G', 'AB', 'R', 'H', '_2B', '_3B', 'HR', 'RBI',
    'SB', 'CS', 'BB', 'SO', 'IBB', 'HBP', 'SH', 'SF', 'GIDP',
]

BATTING_POST_COLUMNS = [
    'yearID', 'round', 'playerID', 'teamID', 'lgID',
    'G', 'AB', 'R', 'H', '_2B', '_3B', 'HR', 'RBI',
    'SB', 'CS', 'BB', 'SO', 'IBB', 'HBP', 'SH', 'SF', 'GIDP',
]

TEAMS_COLUMNS = [
    'yearID', 'lgID', 'teamID', 'franchID', 'divID',
    'Rank', 'G', 'Ghome', 'W', 'L',
    'DivWin', 'WCWin', 'LgWin', 'WSWin',
    'R', 'AB', 'H', '_2B', '_3B', 'HR', 'BB', 'SO', 'SB', 'CS', 'HBP', 'SF',
    'RA', 'ER', 'ERA', 'CG', 'SHO', 'SV', 'IPouts',
    'HA', 'HRA', 'BBA', 'SOA', 'E', 'DP', 'FP',
    'name', 'park', 'attendance', 'BPF', 'PPF',
    'teamIDBR', 'teamIDlahman45', 'teamIDretro',
]

# (table_name, csv_filename, column_override_or_None)
TABLES: list[tuple[str, str, list[str] | None]] = [
    ('all_star_full',         'AllstarFull.csv',         None),
    ('appearances',           'Appearances.csv',         None),
    ('awards_managers',       'AwardsManagers.csv',      None),
    ('awards_players',        'AwardsPlayers.csv',       None),
    ('award_shares_managers', 'AwardsShareManagers.csv', None),
    ('award_shares_players',  'AwardsSharePlayers.csv',  None),
    ('batting',               'Batting.csv',             BATTING_COLUMNS),
    ('batting_post',          'BattingPost.csv',         BATTING_POST_COLUMNS),
    ('college_playing',       'CollegePlaying.csv',      None),
    ('fielding',              'Fielding.csv',            None),
    ('fielding_of',           'FieldingOF.csv',          None),
    ('fielding_of_split',     'FieldingOFsplit.csv',     None),
    ('fielding_post',         'FieldingPost.csv',        None),
    ('franchises',            'TeamsFranchises.csv',     None),
    ('hall_of_fame',          'HallOfFame.csv',          None),
    ('home_games',            'HomeGames.csv',           None),
    ('managers',              'Managers.csv',            None),
    ('managers_half',         'ManagersHalf.csv',        None),
    ('parks',                 'Parks.csv',               None),
    ('people',                'People.csv',              None),
    ('pitching',              'Pitching.csv',            None),
    ('pitching_post',         'PitchingPost.csv',        None),
    ('salaries',              'Salaries.csv',            None),
    ('schools',               'Schools.csv',             None),
    ('series_post',           'SeriesPost.csv',          None),
    ('teams',                 'Teams.csv',               TEAMS_COLUMNS),
    ('teams_half',            'TeamsHalf.csv',           None),
]


def _check_for_unmapped_csvs(data_dir: str) -> None:
    on_disk = {f for f in os.listdir(data_dir) if f.lower().endswith('.csv')}
    mapped = {csv for _, csv, _ in TABLES}
    extras = sorted(on_disk - mapped)
    if extras:
        logging.warning(
            f'Found {len(extras)} CSV(s) in {data_dir} not in the loader map: {extras}. '
            f'If SABR added new tables, update TABLES in lahman.py.'
        )


def _load_csv(path: str, columns: list[str] | None) -> pa.Table:
    logging.info(f'Reading {path}')
    if columns is not None:
        df = pl.read_csv(path, has_header=True, new_columns=columns, infer_schema_length=0)
    else:
        df = pl.read_csv(path, infer_schema_length=0)
    # Lowercase before yielding so dlt's snake_case normalizer doesn't insert underscores
    # mid-identifier (e.g. IPouts -> i_pouts, teamIDlahman45 -> team_i_dlahman45).
    df = df.rename({c: c.lower() for c in df.columns})
    return to_arrow(df, primary_keys=set())


@dlt.source
def lahman(data_dir: str):
    def make_resource(table_name: str, csv_filename: str, columns: list[str] | None):
        def gen() -> Iterator[pa.Table]:
            path = os.path.join(data_dir, csv_filename)
            if not os.path.exists(path):
                raise FileNotFoundError(f'Expected Lahman CSV not found: {path}')
            yield _load_csv(path, columns)
        return dlt.resource(gen, name=table_name, write_disposition='replace')

    for table_name, csv_filename, columns in TABLES:
        yield make_resource(table_name, csv_filename, columns)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', default=DEFAULT_DATA_DIR)
    parser.add_argument('--full-refresh', action='store_true')
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    _check_for_unmapped_csvs(data_dir)

    pipeline = make_pipeline('lahman')
    source = lahman(data_dir=data_dir)

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
