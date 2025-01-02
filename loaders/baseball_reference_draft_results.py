import argparse
import os
import pandas as pd
from pybaseball import amateur_draft

PARSER = argparse.ArgumentParser(description='Download draft results from Baseball-Reference')
PARSER.add_argument('-s', '--start', type=int, default=1965, help='first year to extract from Baseball-Reference')
PARSER.add_argument('-e', '--end', type=int, default=2024, help='last year to extract from Baseball-Reference')
PARSER.add_argument('-t', '--type', type=str, default='junreg', help='type of draft to extract from Baseball-Reference')

ARGS = PARSER.parse_args()

START = ARGS.start
END = ARGS.end + 1
YEARS = range(START, END)
DRAFT_TYPE = ARGS.type

PATH = os.path.dirname(__file__).replace('loaders', 'data/baseball_reference/draft_results')
os.makedirs(PATH, exist_ok=True)

def download_draft_round(year, round, draft_type):
    print(f'Loading Baseball-Reference draft results for {year} round {round}')
    draft_round = amateur_draft(
                      year=year,
                      draft_round=round,
                      draft_type=draft_type,
                      keep_stats=True,
                      keep_columns=True,
                      include_id=True
                      )
    return draft_round

def download_draft_year(year, draft_type=DRAFT_TYPE):
    print(f'Loading Baseball-Reference draft results for {year} {draft_type}')
    round = 0
    drafts = []
    while True:
        round += 1
        try:
            df = download_draft_round(year, round, draft_type)
        except ValueError:
            break
        except ImportError:
            break
        if len(set(df['Year'].values.tolist())) > 1:
            break
        drafts.append(df)
    if len(drafts) > 0:
        df = pd.concat(drafts, ignore_index=True)
        filename = f'baseball_reference_draft_results_{draft_type}_{year}.csv'
        df.rename(columns={"G.1": "GP", "Drafted Out of": "DraftedOutOf"}, inplace=True)
        df.to_csv(f'{PATH}/{filename}', index=False)

def download_draft_results():
    for year in YEARS:
        download_draft_year(year=year)

if __name__ == '__main__':
    download_draft_results()