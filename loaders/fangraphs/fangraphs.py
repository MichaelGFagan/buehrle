import csv
import requests
import os
import json

from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from itertools import product
from prefect import flow, task, get_run_logger
from prefect.futures import wait

BASE_FANGRAPHS_URL = 'https://www.fangraphs.com/api'
CURRENT_PATH = os.path.dirname(__file__)
BASE_DOWNLOAD_PATH = CURRENT_PATH.replace('loaders/fangraphs', 'data/fangraphs')


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
        if self.name == 'REGULAR_SEASON':
            return 'REG'
        elif self.name == 'POSTSEASON':
            return 'POST'
        elif self.name == 'WILD_CARD':
            return 'WC'
        elif self.name == 'DIVISION_SERIES':
            return 'DS'
        elif self.name == 'LEAGUE_CHAMPIONSHIP_SERIES':
            return 'CS'
        elif self.name == 'WORLD_SERIES':
            return 'WS'

class FangraphsEndpoint(Enum):
    MAJOR_LEAGUE_DATA = 'leaders/major-league/data'


@dataclass
class FangraphsData:
    endpoint: FangraphsEndpoint
    season: int
    stat: FangraphsStat
    postseason: FangraphsPlayoff = FangraphsPlayoff.REGULAR_SEASON
    raw_data: list = field(default_factory=list)
    processed_data: list = field(default_factory=list)
    download_path: str = field(init=False)
    filename: str = field(init=False)
    filepath: str = field(init=False)

    def __post_init__(self):
        self.download_path: str = f"{BASE_DOWNLOAD_PATH}/{self.stat}/{self.season}"
        os.makedirs(self.download_path, exist_ok=True)
        self.filename: str = f"fangraphs_{self.stat}_{self.season}_{self.postseason.name.lower()}.csv"
        self.filepath: str = f"{self.download_path}/{self.filename}"


@task(tags=['fangraphs'])
def get_fangraphs_data(data: FangraphsData):
    logger = get_run_logger()
    logger.info(f'Fetching Fangraphs {data.stat} data for the {data.season} {data.postseason}')

    params = {
        'pos': 'all',
        'lg': 'all',
        'stats': data.stat.value,
        'season': data.season,
        'season1': data.season,
        'postseason': data.postseason.value,
        'pageitems': 2000000000,
        'pagenum': 1,
        'ind': 1,
        'team': '0,to', 
        'qual': 0
    }
    url = f"{BASE_FANGRAPHS_URL}/{data.endpoint.value}"
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        raise Exception(f"Error fetching data from Fangraphs: {response.status_code}")
    
    data.raw_data = response.json()['data']

    if not data.raw_data:
        logger.warning(f'No data found for {data.stat} in {data.season} {data.postseason}')

    return data


@task
def process_fangraphs_data(data: FangraphsData):
    logger = get_run_logger()
    logger.info(f'Processing Fangraphs data for {data.filename}')

    processed_data = []
    for row in data.raw_data:
        extracted = str(datetime.now())
        row.pop('Name')
        row.pop('Team')
        row['leg'] = data.postseason.string_abbreviation()
        row['is_postseason'] = data.postseason != FangraphsPlayoff.REGULAR_SEASON
        processed_data.append({'record_content': json.dumps(row), 'extracted_at': f"{extracted}"})
    data.processed_data = processed_data
    return data


@task
def save_fangraphs_data_to_csv(data: FangraphsData):
    logger = get_run_logger()
    logger.info(f'Saving Fangraphs data to CSV: {data.filepath}')

    headers = ['record_content', 'extracted_at']
    with open(data.filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, quotechar = "'")
        writer.writeheader()
        for row in data.processed_data:
            writer.writerow(row)
        

@task
def get_season_stats_to_download(
    start_season: int,
    end_season:int,
    stats: list, 
    postseason: list[FangraphsPlayoff] = [FangraphsPlayoff.REGULAR_SEASON],
    endpoints: list[FangraphsEndpoint] = [FangraphsEndpoint.MAJOR_LEAGUE_DATA]
):
    logger = get_run_logger()
    logger.info(f'Getting year and seasons stats to download from {start_season} to {end_season}')

    end_season += 1
    seasons = range(start_season, end_season)
    configs = product(endpoints, seasons, stats, postseason)
    return [FangraphsData(config[0], config[1], config[2], config[3]) for config in configs]


@flow
def download_fangraphs_data(
    start_season: int,
    end_season: int,
    stats: list[FangraphsStat],
    postseason: list[FangraphsPlayoff] = [FangraphsPlayoff.REGULAR_SEASON],
    endpoints: list[FangraphsEndpoint] = [FangraphsEndpoint.MAJOR_LEAGUE_DATA],
    chunk_size: int = 5
):
    logger = get_run_logger()
    logger.info(f'Loading Fangraphs data from {start_season} to {end_season}')
    
    season_stats = get_season_stats_to_download(start_season, end_season, stats, postseason, endpoints)
    season_stats_chunks = [season_stats[i:i + chunk_size] for i in range(0, len(season_stats), chunk_size)]
    for chunk in season_stats_chunks:
        downloaded_data = get_fangraphs_data.map(chunk)
        wait(downloaded_data)
        valid_downloaded_data = [data for data in downloaded_data if data.result().raw_data]
        processed_data = process_fangraphs_data.map(valid_downloaded_data)
        wait(processed_data)
        saved_data = save_fangraphs_data_to_csv.map(processed_data)
        wait(saved_data)


if __name__ == "__main__":
    download_fangraphs_data(
        1871,
        2025,
        [FangraphsStat.BATTING, FangraphsStat.PITCHING, FangraphsStat.FIELDING],
        [
            # FangraphsPlayoff.REGULAR_SEASON,
            # FangraphsPlayoff.WILD_CARD,
            # FangraphsPlayoff.DIVISION_SERIES,
            # FangraphsPlayoff.LEAGUE_CHAMPIONSHIP_SERIES,
            # FangraphsPlayoff.WORLD_SERIES,
            FangraphsPlayoff.POSTSEASON
        ],)
