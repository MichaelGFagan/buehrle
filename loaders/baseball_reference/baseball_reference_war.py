import logging
import os
import sys
from io import StringIO

import dlt
import pandas as pd

from dlt.sources.helpers import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from dlt_utils import make_pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

URLS = {
    'batting': 'https://www.baseball-reference.com/data/war_daily_bat.txt',
    'pitching': 'https://www.baseball-reference.com/data/war_daily_pitch.txt',
}


def _make_resource(stat: str, url: str):

    @dlt.resource(name=f'war_{stat}', write_disposition='replace')
    def _resource():
        logging.info(f'Fetching baseball reference {stat} WAR')
        response = requests.get(url)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        yield from df.to_dict(orient='records')

    return _resource


@dlt.source
def baseball_reference_war():
    for stat, url in URLS.items():
        yield _make_resource(stat, url)


if __name__ == '__main__':
    pipeline = make_pipeline('baseball_reference')

    load_info = pipeline.run(baseball_reference_war())
    print(load_info)
