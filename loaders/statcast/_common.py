import datetime
import logging
import time
from collections.abc import Callable, Iterator

import dlt
import polars as pl
import requests

from loaders.dlt_utils import to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

TODAY = datetime.date.today()

SAVANT_HOST = 'https://baseballsavant.mlb.com'
BASE_URL = f'{SAVANT_HOST}/leaderboard'

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'  # noqa: E501
REQUEST_TIMEOUT = 60
SLEEP_BETWEEN = 1


def fetch_csv(url: str) -> pl.DataFrame | None:
    response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    df = pl.read_csv(response.content, infer_schema=False)
    return df if df.height > 0 else None


def inject_labels(df: pl.DataFrame, labels: dict) -> pl.DataFrame:
    """Stamp query-parameter labels (year, position, ...) onto every row.

    A label is the value we queried for, so it's authoritative. We add it when
    the CSV lacks the column, and also overwrite an existing column that the
    source left entirely blank/null - e.g. Savant's ``outs_above_average`` ships
    an empty ``year`` column, which would otherwise blank the loader's watermark
    and force a full-history reload next run.
    """
    additions = []
    for k, v in labels.items():
        if k not in df.columns or df[k].str.strip_chars().replace('', None).null_count() == df.height:
            additions.append(pl.lit(str(v)).alias(k))
    return df.with_columns(additions) if additions else df


def run_years(
    name: str,
    pks: set[str],
    start_year: int,
    end_year: int,
    iter_fn,
    update: bool = False,
    fetcher: Callable[[str], pl.DataFrame | None] = fetch_csv,
) -> Iterator:
    state = dlt.current.resource_state()
    from_year = start_year if update else state.get('last_year', start_year)
    for year in range(from_year, end_year + 1):
        for labels, url in iter_fn(year):
            logging.info(f'Fetching {name} {year} {labels if labels else ""}')
            df = fetcher(url)
            if df is None:
                continue
            df = inject_labels(df, {'year': year, **labels})
            yield to_arrow(df, pks)
            time.sleep(SLEEP_BETWEEN)
        state['last_year'] = year
