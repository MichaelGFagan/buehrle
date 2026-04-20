import os
import string
import requests
from prefect import task, flow, get_run_logger
from prefect.runtime import task_run
from prefect.states import Failed, Completed

CHARACTERS = [str(num) for num in range(10)] + list(string.ascii_lowercase)
PATH = os.path.dirname(__file__).replace('loaders/chadwick', 'data/chadwick/register')
BASE_URL = f'https://raw.githubusercontent.com/chadwickbureau/register/refs/heads/master/data'
os.makedirs(PATH, exist_ok=True)

@task(task_run_name='get_chadwick_file_suffixes')
def get_chadwick_file_suffixes():
    logger = get_run_logger()
    logger.info('Getting valid Chadwick file suffixes')
    
    valid_characters = []

    for character in CHARACTERS:
        
        try:
            response = requests.get(f'{BASE_URL}/people-{character}.csv')    
        except requests.exceptions.ConnectionError as error:
            error_message = str(error).split(': ')
            error_message = error_message[-1].split('(')[0]
            error_message = f'Connection error: {error_message}'
            logger.error(error_message)
            return []
        else:
            if response.status_code == 200:
                valid_characters.append(character)
            elif character == next(iter(CHARACTERS)):
                error_message = f'Data may have moved from {BASE_URL}. Please check https://github.com/chadwickbureau/register.'
                logger.error(error_message)
                return []
            else:
                break
    
    return valid_characters

def generate_download_chadwick_register_file_task_name():
    task_name = task_run.task_name
    parameters = task_run.parameters
    suffix = parameters['suffix']

    return f"{task_name}('{suffix}')"

@task(task_run_name=generate_download_chadwick_register_file_task_name, tags=['test'])
def download_chadwick_register_file(suffix):
    logger = get_run_logger()
    
    filename = f'people-{suffix}.csv'
    url = f'{BASE_URL}/{filename}'
    filepath = f'{PATH}/{filename}'
    logger.info(f'Downloading {filename}...')

    try:
        with requests.get(url, stream=True) as request:
            request.raise_for_status()
            with open(filepath, 'wb') as file:
                for chunk in request.iter_content(chunk_size=8192):
                    file.write(chunk)
    except requests.exceptions.HTTPError as error:
        error_message = str(error).split(': ')
        error_message = error_message[-1].split('(')[0]
        error_message = f'Connection error: {error_message}'
        logger.error(error_message)
        return Failed(message=error_message)

    return Completed()

@flow()
def download_chadwick_register():
    suffixes = get_chadwick_file_suffixes()
    futures = download_chadwick_register_file.map(suffixes)
    return futures.result()

if __name__ == '__main__':
    download_chadwick_register()
