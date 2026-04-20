import argparse
import datetime
import os
import pandas as pd
from pybaseball import statcast
from calendar import monthrange
from prefect import flow, task, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

TODAY = datetime.date.today()
THIS_YEAR = TODAY.year
THIS_MONTH = TODAY.month

PARSER = argparse.ArgumentParser(description="Download Statcast data into a CSV")
PARSER.add_argument('-s', '--start', type=int, default=THIS_YEAR, help="first year to extract from Statcast")
PARSER.add_argument('-e', '--end', type=int, help="last year to extract from Statcast")

ARGS = PARSER.parse_args()
START = ARGS.start
if ARGS.end:
    END = ARGS.end + 1
else:
    END = ARGS.start + 1

PATH = os.path.dirname(__file__).replace('loaders', 'data/statcast')
os.makedirs(PATH, exist_ok=True)

@task
def get_statcast_year(year):
    df_list = []
    end_month = 12
    if year == TODAY.year:
        end_month = TODAY.month + 1
    for month in range(3, end_month):
        start_date = datetime.date(year, month, 1).strftime("%Y-%m-%d")
        if year == THIS_YEAR and month == THIS_MONTH:
            end_date = TODAY.strftime("%Y-%m-%d")
        else:
            end_date = datetime.date(year, month, monthrange(year, month)[1]).strftime("%Y-%m-%d")
        try:
            df = statcast(start_date, end_date)
        except:
            pass
        df_list.append(df)
    df_dict = {'year': year, 'df_list': df_list}
    return df_dict

@task
def process_statcast_dataframe(df_dict):
    year = df_dict['year']
    df_list = df_dict['df_list']

    filename = f"statcast_{year}.csv"

    df = pd.DataFrame()
    for df_ in df_list:
        if len(df_.index) > 0:
            df = pd.concat([df, df_])
    
    df.rename(columns={"pitcher.1": "pitcher_1", "fielder_2.1": "fielder_2_1"}, inplace=True)
    df.to_csv(f'{PATH}/{filename}', index=False)

@flow(task_runner=ThreadPoolTaskRunner(max_workers=3))
def download_statcast(start_year=START, end_year=END):
    years = range(start_year, end_year)

    downloaded_data = get_statcast_year.map(years)
    process_statcast_dataframe.map(downloaded_data).result()


if __name__ == '__main__':
    download_statcast(START, END)