import logging
import os
import re
import sys
import time
import dlt

from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNaming
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers import requests
from enum import Enum
from itertools import product
from typing import Iterator

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

BASE_FANGRAPHS_URL = 'https://www.fangraphs.com/api/leaders/major-league/data'


class FangraphsNamingConvention(SnakeCaseNaming):

    RENAMES = {
        '1B':       '_1b',
        '2B':       '_2b',
        '3B':       '_3b',
        '-WPA':     'neg_wpa',
        '+WPA':     'pos_wpa',
        'FB%1':     'fb_pct',
        'C+SwStr%': 'c_plus_sw_str_pct',
        'CFraming': 'c_framing',
        'WAROld':   'war_old',
        'GDPRuns':  'gdp_runs',
        'rFTeamV':  'r_f_team_v',
        'rBTeamV':  'r_b_team_v',
        'CStr%':    'c_str_pct',
        'xMLBAMID': 'mlbamid',
        'ShO':      'sho',
        'E-F':      'era_minus_fip',
        'CStrikes': 'c_strikes',
        'TInn':     't_inn',
    }

    PREFIXES = ('pfx', 'sc', 'pi')

    def _apply_rules(self, identifier: str) -> str:
        if identifier.startswith('-'):
            identifier = 'neg_' + identifier[1:]
        if identifier.endswith('-'):
            identifier = identifier[:-1] + '_minus'
        identifier = identifier.replace('%', '_pct')
        identifier = identifier.replace('+', '_plus')
        identifier = identifier.replace('/', '_per_')
        identifier = re.sub(r'([A-Z]{2,})([a-z])', lambda m: m.group(1) + '_' + m.group(2), identifier)
        return identifier

    def normalize_identifier(self, identifier: str) -> str:
        if identifier in self.RENAMES:
            return self.RENAMES[identifier]
        for prefix in self.PREFIXES:
            if identifier.startswith(prefix) and len(identifier) > len(prefix):
                rest = identifier[len(prefix):]
                return prefix + '_' + super().normalize_identifier(self._apply_rules(rest))
        return super().normalize_identifier(self._apply_rules(identifier))
DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/buehrle.duckdb')


class FangraphsStat(Enum):
    BATTING = 'bat'
    PITCHING = 'pit'
    FIELDING = 'fld'

    def __str__(self):
        return self.name.lower()


class FangraphsPlayoff(Enum):
    REGULAR_SEASON = ''
    WILD_CARD = 'F'
    DIVISION_SERIES = 'D'
    LEAGUE_CHAMPIONSHIP_SERIES = 'L'
    WORLD_SERIES = 'W'
    POSTSEASON = 'Y'

    def __str__(self):
        if self.name == 'WILD_CARD':
            return 'Wild Card Series'
        elif self.name in ['REGULAR_SEASON', 'POSTSEASON']:
            return self.name.replace('_', ' ').lower()
        else:
            return self.name.replace('_', ' ').title()

    def string_abbreviation(self):
        return {
            'REGULAR_SEASON': 'REG',
            'POSTSEASON': 'POST',
            'WILD_CARD': 'WC',
            'DIVISION_SERIES': 'DS',
            'LEAGUE_CHAMPIONSHIP_SERIES': 'CS',
            'WORLD_SERIES': 'WS',
        }[self.name]


def _fetch_season(stat: FangraphsStat, season: int, playoff: FangraphsPlayoff) -> list[dict]:
    params = {
        'pos': 'all',
        'lg': 'all',
        'stats': stat.value,
        'season': season,
        'season1': season,
        'postseason': playoff.value,
        'pageitems': 2000000000,
        'pagenum': 1,
        'ind': 1,
        'team': '0,to',
        'qual': 0,
    }
    response = requests.get(BASE_FANGRAPHS_URL, params=params)
    response.raise_for_status()
    return response.json().get('data', [])


def _make_resource(stat: FangraphsStat, start_season: int, end_season: int, postseason: list[FangraphsPlayoff], update: bool = False):

    @dlt.resource(
        name=str(stat),
        write_disposition='merge',
        primary_key=['playerid', 'Season', 'Team', 'leg'],
    )
    def _resource() -> Iterator[dict]:
        state = dlt.current.resource_state()
        from_season = start_season if update else state.get('last_season', start_season)
        for season, playoff in product(range(from_season, end_season + 1), postseason):
            logging.info(f'Fetching {stat} {season} {playoff}')
            rows = _fetch_season(stat, season, playoff)
            extras = {'leg': playoff.string_abbreviation(), 'is_postseason': playoff != FangraphsPlayoff.REGULAR_SEASON}
            yield from ({**row, **extras} for row in rows)
            time.sleep(2)
            state['last_season'] = season

    return _resource


@dlt.source
def fangraphs(
    start_season: int,
    end_season: int,
    stats: tuple[FangraphsStat] = (FangraphsStat.BATTING, FangraphsStat.PITCHING, FangraphsStat.FIELDING),
    postseason: tuple[FangraphsPlayoff] = (FangraphsPlayoff.REGULAR_SEASON,),
    update: bool = False,
):
    for stat in stats:
        yield _make_resource(stat, start_season, end_season, postseason, update)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name='fangraphs',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='fangraphs',
    )

    source = fangraphs(
        start_season=1871,
        end_season=2026,
        stats=[FangraphsStat.BATTING, FangraphsStat.PITCHING, FangraphsStat.FIELDING],
        # postseason=[FangraphsPlayoff.REGULAR_SEASON],
        postseason=[FangraphsPlayoff.REGULAR_SEASON, FangraphsPlayoff.WILD_CARD, FangraphsPlayoff.DIVISION_SERIES, FangraphsPlayoff.LEAGUE_CHAMPIONSHIP_SERIES, FangraphsPlayoff.WORLD_SERIES],
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
