import argparse
import logging
import string
import dlt
import polars as pl
import pyarrow as pa
import requests

from typing import Iterator

from loaders.dlt_utils import handle_full_refresh, make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

CHARACTERS = [str(num) for num in range(10)] + list(string.ascii_lowercase)
BASE_URL = 'https://raw.githubusercontent.com/chadwickbureau/register/refs/heads/master/data'
PRIMARY_KEYS = {'key_uuid'}


@dlt.resource(name='people', write_disposition='replace', primary_key='key_uuid')
def people() -> Iterator[pa.Table]:
    for i, character in enumerate(CHARACTERS):
        url = f'{BASE_URL}/people-{character}.csv'
        logging.info(f'Fetching people-{character}.csv')
        response = requests.get(url, timeout=30)

        if response.status_code == 404:
            if i == 0:
                logging.error(
                    f'Data may have moved from {BASE_URL}. '
                    'Please check https://github.com/chadwickbureau/register.'
                )
                return
            break

        response.raise_for_status()
        df = pl.read_csv(response.content, infer_schema=False)
        yield to_arrow(df, PRIMARY_KEYS)


@dlt.source
def chadwick_register():
    yield people()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--full-refresh', action='store_true')
    args = parser.parse_args()

    pipeline = make_pipeline('chadwick_register')

    source = chadwick_register()

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
