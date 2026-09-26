import html
import logging
import re
from io import StringIO

import dlt
import pandas as pd
from dlt.sources.helpers import requests

from loaders.cli import add_resources_arg, run_loader
from loaders.dlt_utils import baseball_reference_request_kwargs, make_pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

URLS = {
    'batting': 'https://www.baseball-reference.com/data/war_daily_bat.txt',
    'pitching': 'https://www.baseball-reference.com/data/war_daily_pitch.txt',
}

PIPELINE_NAME = 'baseball_reference_war'  # destination schema (== dlt pipeline/dataset name)
# Single-shot replace loader: full-refresh only, no incremental watermark.
WATERMARKS: dict[str, str] = {}


def _extract_body_text(response) -> str:
    """Return the file's plain text, unwrapping the HTML the IPRoyal Unblocker
    proxy adds when it renders a .txt file through a headless browser (the
    same '<html><body><pre>...' wrapper Chrome/Firefox show for a raw text
    file opened directly). A no-op when hitting the file directly, since the
    Content-Type won't be HTML in that case.
    """
    if 'html' in response.headers.get('Content-Type', '').lower():
        match = re.search(r'<pre[^>]*>(.*)</pre>', response.text, re.DOTALL)
        if match:
            return html.unescape(match.group(1))
    return response.text


def _make_resource(stat: str, url: str):

    @dlt.resource(name=f'war_{stat}', write_disposition='replace')
    def _resource():
        logging.info(f'Fetching baseball reference {stat} WAR')
        response = requests.get(url, timeout=30, **baseball_reference_request_kwargs())
        response.raise_for_status()
        df = pd.read_csv(StringIO(_extract_body_text(response)))
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
    pipeline = make_pipeline(PIPELINE_NAME)

    run_loader(pipeline, baseball_reference_war(), args)
