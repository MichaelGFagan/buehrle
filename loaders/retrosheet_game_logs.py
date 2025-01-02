import argparse
import os
from pybaseball import retrosheet as rs

PARSER = argparse.ArgumentParser(description="Download Retrosheet game log data into a CSV")
PARSER.add_argument('-s', '--start', type=int, default=1871, help="first year to extract from Retrosheet")
PARSER.add_argument('-e', '--end', type=int, default=2023, help="last year to extract from Retrosheet")
PARSER.add_argument('-p', '--playoffs', action='store_true', help="include playoffs and all-star game logs from Retrosheet")

ARGS = PARSER.parse_args()

START = ARGS.start
END = ARGS.end + 1
PLAYOFFS = ARGS.playoffs
YEARS = range(START, END)

PLAYOFF_GAME_LOG_FUNCTIONS = {
    'world_series_games': rs.world_series_logs,
    'all_star_games': rs.all_star_game_logs,
    'wild_card_games': rs.wild_card_logs,
    'division_series_games': rs.division_series_logs,
    'league_championship_games': rs.lcs_logs
}

PATH = os.path.dirname(__file__).replace('loaders', 'data/retrosheet/game_logs')
os.makedirs(PATH, exist_ok=True)


def get_retrosheet_df(scope, kwargs):
    kwarg = kwargs[scope]
    print(f'Loading Retrosheet {kwarg} game logs')
    filename = f'retrosheet_{kwarg}.csv'
    try:
        if scope == 'year':
            filename = f'regular_season_games/{filename}'
            return rs.season_game_logs(kwarg), filename
        elif scope == 'games':
            return PLAYOFF_GAME_LOG_FUNCTIONS[kwarg](), filename
    except:
        print(f'Could not load Retrosheet {kwarg}.')

def transform_and_load(**kwargs):
    if 'year' in kwargs:
        df, filename = get_retrosheet_df('year', kwargs)
    elif 'games' in kwargs:
        df, filename = get_retrosheet_df('games', kwargs)
    else:
        raise ValueError('Missing year or games keyword argument.')
    df.rename(columns={"visiting_2_id.1": "visiting_3_id"}, inplace=True)
    # table_schema = []
    # for column in df.columns:
    #     table_schema.append({'name': column, 'type': 'STRING'})
    df.to_csv(f'{PATH}/{filename}', index=False)

def download_retrosheet():
    for year in YEARS:
        transform_and_load(year=year)

    if PLAYOFFS:
        for games in PLAYOFF_GAME_LOG_FUNCTIONS:
            transform_and_load(games=games)

if __name__ == '__main__':
    download_retrosheet()