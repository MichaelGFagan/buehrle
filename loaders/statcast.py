import argparse
import datetime
import os
import pandas as pd
from pybaseball import statcast
from calendar import monthrange

TODAY = datetime.date.today()
THIS_YEAR = TODAY.year
THIS_MONTH = TODAY.month

PARSER = argparse.ArgumentParser(description="Download Statcast data into a CSV")
PARSER.add_argument('-s', '--start', type=int, default=THIS_YEAR, help="first year to extract from Statcast")
PARSER.add_argument('-e', '--end', type=int, help="last year to extract from Statcast")

ARGS = PARSER.parse_args()
START = ARGS.start
if ARGS.end:
    END = ARGS.end
else:
    END = ARGS.start

PATH = os.path.dirname(__file__).replace('loaders', 'data/statcast')
os.makedirs(PATH, exist_ok=True)

def get_statcast_year(year, end_month_range):
    df_dict = {}
    for month in range(3, end_month_range):
        start_date = datetime.date(year, month, 1).strftime("%Y-%m-%d")
        if year == THIS_YEAR and month == THIS_MONTH:
            end_date = TODAY.strftime("%Y-%m-%d")
        else:
            end_date = datetime.date(year, month, monthrange(year, month)[1]).strftime("%Y-%m-%d")
        try:
            df = statcast(start_date, end_date)
        except:
            pass
        df_dict[month] = df
    return df_dict

def collate_statcast_dataframe(df_dict):
    data = pd.DataFrame()
    for df in list(df_dict.keys()):
        if len(df_dict[df].index) > 0:
            data = pd.concat([data, df_dict[df]])
    return data

def download_statcast(start_year=START, end_year=END):
    for year in range(start_year, end_year):
        end_month_range = 12
        if year == TODAY.year:
            end_month_range = TODAY.month + 1
        filename = f"statcast_{year}.csv"
        year_df_dict = get_statcast_year(year, end_month_range)
        df = collate_statcast_dataframe(year_df_dict)
        df.rename(columns={"pitcher.1": "pitcher_1", "fielder_2.1": "fielder_2_1"}, inplace=True)
        df.to_csv(f'{PATH}/{filename}', index=False)

if __name__ == '__main__':
    download_statcast(START, END)