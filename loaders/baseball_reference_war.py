import os
import pandas as pd

FILES_DICT = {
    'baseball_reference_batting_war': 'https://www.baseball-reference.com/data/war_daily_bat.txt',
    'baseball_reference_pitching_war': 'https://www.baseball-reference.com/data/war_daily_pitch.txt'
}
PATH = os.path.dirname(__file__).replace('loaders', 'data/baseball_reference/war')


os.makedirs(PATH, exist_ok=True)
def bbref_war():
    for file in FILES_DICT:
        print('Loading bbref ' + file.split('_')[2] + ' WAR csv')
        url = FILES_DICT[file]
        df = pd.read_csv(url)
        df.to_csv(f'{PATH}/{file}.csv', index=False)

if __name__ == '__main__':
    bbref_war()