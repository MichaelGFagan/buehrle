import datetime
import logging
import os
import time
import dlt
import polars as pl
import pyarrow as pa
import requests

from typing import Callable, Iterator

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/buehrle.duckdb')
TODAY = datetime.date.today()

SAVANT_HOST = 'https://baseballsavant.mlb.com'
BASE_URL = f'{SAVANT_HOST}/leaderboard'

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'
REQUEST_TIMEOUT = 60
SLEEP_BETWEEN = 1


def fetch_csv(url: str) -> pl.DataFrame | None:
    response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    df = pl.read_csv(response.content, infer_schema=False)
    return df if df.height > 0 else None


def inject_labels(df: pl.DataFrame, labels: dict) -> pl.DataFrame:
    existing = set(df.columns)
    additions = [pl.lit(str(v)).alias(k) for k, v in labels.items() if k not in existing]
    return df.with_columns(additions) if additions else df


def to_arrow(df: pl.DataFrame, primary_keys: set[str]) -> pa.Table:
    table = df.to_arrow()
    schema = pa.schema([
        f.with_type(pa.utf8()).with_nullable(f.name not in primary_keys)
        if f.type == pa.large_utf8()
        else f
        for f in table.schema
    ])
    return table.cast(schema)


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
