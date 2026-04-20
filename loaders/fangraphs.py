import argparse
import os
import json
from pybaseball import batting_stats, pitching_stats
from prefect import flow, task, get_run_logger, unmapped
from prefect.states import Failed, Completed

parser = argparse.ArgumentParser(description="Download Fangraphs data into a CSV")
parser.add_argument('-s', '--start', type=int, default=1871, help="first year to extract from Fangraphs")
parser.add_argument('-e', '--end', type=int, default=2026, help="last year to extract from Fangraphs")

args = parser.parse_args()
start_year = args.start
end_year = args.end

CURRENT_PATH = os.path.dirname(__file__)
DOWNLOAD_PATH = CURRENT_PATH.replace('fangraphs/loaders', 'data/fangraphs')
os.makedirs(DOWNLOAD_PATH, exist_ok=True)


with open(f'{CURRENT_PATH}/helpers/fangraphs_columns_renamed.json', 'r') as file:
    RENAMED_COLUMNS = json.load(file)


def fangraphs_function(side):
    if side == 'batting':
        return batting_stats
    elif side == 'pitching':
        return pitching_stats

@task
def get_year_sides_to_download(start_year, end_year):
    logger = get_run_logger()
    logger.info(f'Getting year and side list from {start_year} to {end_year}')
    end_year += 1
    years = range(start_year, end_year)
    return [{"year": year, "side": side} for year in years for side in ['batting', 'pitching']]

@task(tags=['test'])
def download_fangraphs_year_side(config):
    year = config['year']
    side = config['side']
    logger = get_run_logger()
    logger.info(f'Downloading Fangraphs data for {year} {side}')
    stats = fangraphs_function(side)
    config["df"] = stats(year, qual=0, split_teams=True)
    return config

@task
def save_fangraphs_year_side_to_csv(config):
    df = config['df']
    year = config['year']
    side = config['side']
    filename = f'fangraphs_{side}_{str(year)}.csv'
    df.rename(columns=RENAMED_COLUMNS, inplace=True)
    df.to_csv(f'{DOWNLOAD_PATH}/{side}/{filename}', index=False)

    return Completed()

@flow
def download_fangraphs(start_year, end_year):
    logger = get_run_logger()
    logger.info(f'Loading Fangraphs data from {start_year} to {end_year}')
    
    year_sides = get_year_sides_to_download(start_year, end_year)
    downloaded_data = download_fangraphs_year_side.map(year_sides)
    
    return save_fangraphs_year_side_to_csv.map(downloaded_data).result()


if __name__ == '__main__':
    download_fangraphs(start_year, end_year)