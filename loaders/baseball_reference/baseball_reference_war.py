import logging
from io import StringIO

import dlt
import pandas as pd

from dlt.sources.helpers import requests

from loaders.cli import add_resources_arg, apply_resources
from loaders.dlt_utils import handle_full_refresh, make_pipeline

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


def register(subparsers):
    parser = subparsers.add_parser('baseball-reference-war', help='Baseball-Reference WAR')
    parser.add_argument('--full-refresh', action='store_true')
    add_resources_arg(parser)
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    pipeline = make_pipeline('baseball_reference')

    source = apply_resources(baseball_reference_war(), args)

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
