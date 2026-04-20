"""
Fetches one season from the FanGraphs API and compares original column names
against what our custom naming convention would produce.

Usage: python check_column_names.py [bat|pit|fld]  (default: bat)
"""
import re
import sys
import requests

from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNaming

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


params = {
    'pos': 'all',
    'lg': 'all',
    'stats': sys.argv[1] if len(sys.argv) > 1 else 'bat',
    'season': 2024,
    'season1': 2024,
    'postseason': '',
    'pageitems': 1,
    'pagenum': 1,
    'ind': 0,
    'team': '0,to',
    'qual': 0,
}

response = requests.get(BASE_FANGRAPHS_URL, params=params)
response.raise_for_status()

columns = response.json()['data'][0].keys()
convention = FangraphsNamingConvention()

rows = [(col, convention.normalize_identifier(col)) for col in columns]
changed = [(original, normalized) for original, normalized in rows if original != normalized]
unchanged = [original for original, normalized in rows if original == normalized]

print(f'{len(changed)} columns will be renamed, {len(unchanged)} unchanged\n')
print(f'{"Original":<30} {"Normalized":<30}')
print('-' * 60)
for original, normalized in changed:
    print(f'{original:<30} {normalized:<30}')
