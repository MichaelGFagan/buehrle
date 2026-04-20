import argparse
import os
import shutil
import json
import pandas as pd
from pybaseball import amateur_draft
from prefect import task, flow, get_run_logger, concurrency, unmapped
from prefect.states import Failed, Completed
from prefect.concurrency.sync import rate_limit

PARSER = argparse.ArgumentParser(description='Download draft results from Baseball-Reference')
PARSER.add_argument('-s', '--start', type=int, default=1965, help='first year to extract from Baseball-Reference')
PARSER.add_argument('-e', '--end', type=int, default=2025, help='last year to extract from Baseball-Reference')
PARSER.add_argument('--no-junreg', action='store_true', help='exclude June draft')
PARSER.add_argument('--augleg', action='store_true', help='include August Legion draft')
PARSER.add_argument('--janreg', action='store_true', help='include January regular draft')
PARSER.add_argument('--jansec', action='store_true', help='include January secondary draft')
PARSER.add_argument('--junsec', action='store_true', help='include June secondary draft')
PARSER.add_argument('--all', action='store_true', help='include all drafts')

ARGS = PARSER.parse_args()

START_YEAR = ARGS.start
END_YEAR = ARGS.end

DRAFT_TYPES = ['junreg']
if ARGS.augleg: DRAFT_TYPES.append('augleg')
if ARGS.janreg: DRAFT_TYPES.append('janreg')
if ARGS.jansec: DRAFT_TYPES.append('jansec')
if ARGS.junsec: DRAFT_TYPES.append('junsec')
if ARGS.all: DRAFT_TYPES = ['junreg', 'augleg', 'janreg', 'jansec', 'junsec']

CURRENT_PATH = os.path.dirname(__file__)
PATH = CURRENT_PATH.replace('loaders', 'data/baseball_reference/draft_results')
os.makedirs(PATH, exist_ok=True)

with open(f'{CURRENT_PATH}/helpers/baseball_reference_draft_years.json', 'r') as file:
    DRAFT_YEARS = json.load(file)

@task
def process_draft(draft, draft_rounds):
    year = draft['year']
    draft_type = draft['draft_type']
    filename = f'baseball_reference_draft_results_{draft_type}_{year}.csv'

    logger = get_run_logger()
    logger.info(f'Processing Baseball-Reference draft results for {year} {draft_type}')

    dfs = [draft_round['df'] for draft_round in draft_rounds if draft_round['year'] == year and draft_round['draft_type'] == draft_type]

    df = pd.concat(dfs, ignore_index=True)
    df.rename(columns={"G.1": "GP", "Drafted Out of": "DraftedOutOf"}, inplace=True)
    df.to_csv(f'{PATH}/{filename}', index=False)

    return Completed()

@task
def download_draft_year_type_round(draft_round):
    rate_limit('bbref_drafts')

    year = draft_round['year']
    draft_type = draft_round['draft_type']
    round = draft_round['round']

    logger = get_run_logger()
    logger.info(f'Loading Baseball-Reference draft results for {year} {draft_type} round {round}')

    try:
        df = amateur_draft(
            year=year,
            draft_round=round,
            draft_type=draft_type,
            keep_stats=True,
            keep_columns=True,
            include_id=True
            )
    except ValueError:
        pass
    except ImportError:
        pass
    if df.empty or (len(set(df['Year'].values.tolist())) > 1):
        return None
    draft_round['df'] = df

    return draft_round

@task
def get_draft_rounds_to_download(draft):
    year = draft['year']
    draft_type = draft['draft_type']
    rounds = draft['rounds']
    logger = get_run_logger()
    logger.info(f'Checking available draft rounds for {year} {draft_type}')

    draft_rounds = []
    for round in range(1, rounds + 1):
        draft_rounds.append({"year": year, "draft_type": draft_type, "round": round})

    return draft_rounds

@task
def get_drafts_to_download(start_year, end_year, draft_types):
    logger = get_run_logger()
    logger.info(f'Getting drafts to download from {start_year} to {end_year}')
    years = range(start_year, end_year + 1)
    drafts = []
    for draft_type in draft_types:
        logger.info(f'Checking available draft years for type: {draft_type}')
        start_year = DRAFT_YEARS[draft_type]['start_year']
        end_year = DRAFT_YEARS[draft_type]['end_year']
        draft_lengths = DRAFT_YEARS[draft_type]['draft_lengths']
        for year in years:
            if year >= start_year and year <= end_year:
                rounds = 0
                for draft_length in draft_lengths:
                    draft_start_year = draft_length[0]
                    draft_end_year = draft_length[1]
                    if year >= draft_start_year and year <= draft_end_year:
                        rounds = draft_length[2]
                        logger.info(f'Adding {year} {draft_type} with {rounds} rounds')
                        drafts.append({"year": year, "draft_type": draft_type, "rounds": rounds})
    return drafts

@task
def copy_1971_junsec_drafts():
    override_path = f'{CURRENT_PATH}/helpers'
    files = ['baseball_reference_draft_results_junsec_1971.csv', 'baseball_reference_draft_results_junsecd_1971.csv']
    for file in files:
        shutil.copy(f'{override_path}/{file}', f'{PATH}/{file}')

@flow
def download_baseball_reference_draft_results(start_year, end_year, draft_types=['junreg']):
    should_run_1971_task = True if (start_year <= 1971 and end_year >= 1971) and 'junsec' in draft_types else False
    
    drafts = get_drafts_to_download(start_year, end_year, draft_types)
    for draft in drafts:
        draft_rounds = get_draft_rounds_to_download(draft)
        downloaded_draft_rounds = download_draft_year_type_round.map(draft_rounds).result()
        process_draft(draft, downloaded_draft_rounds).result()
    
    if should_run_1971_task:
        copy_1971_junsec_drafts()


if __name__ == '__main__':
    download_baseball_reference_draft_results(START_YEAR, END_YEAR, DRAFT_TYPES)
