import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
import dlt
import pandas as pd
import polars as pl
import pyarrow as pa

import requests
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from enum import Enum
from io import StringIO
from typing import Iterator

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/buehrle.duckdb')
HELPERS_PATH = os.path.join(os.path.dirname(__file__), '../helpers')

BASE_URL = 'https://www.baseball-reference.com/draft/?year_ID={year}&draft_round={round}&draft_type={draft_type}&query_type=year_round'

COLUMN_RENAMES = {
    'G.1': 'GP',
    'Drafted Out of': 'DraftedOutOf',
}

PRIMARY_KEYS = {'Year', 'draft_type', 'Rnd', 'RdPck'}
RETRY_BACKOFF = (30, 60, 120)

with open(os.path.join(HELPERS_PATH, 'baseball_reference_draft_years.json')) as f:
    DRAFT_YEARS = json.load(f)


class DraftType(Enum):
    JUNREG = 'junreg'
    AUGLEG = 'augleg'
    JANREG = 'janreg'
    JANSEC = 'jansec'
    JUNSEC = 'junsec'

    def __str__(self):
        return self.value


def _rounds_for_year(draft_type: str, year: int) -> int:
    for start, end, rounds in DRAFT_YEARS[draft_type]['draft_lengths']:
        if start <= year <= end:
            return rounds
    return 0


def _clean_with_links(df: pd.DataFrame) -> pd.DataFrame:
    """Extract text and href from tuple cells produced by pd.read_html(extract_links='body')."""
    def _href(val):
        return val[1] if isinstance(val, tuple) and val[1] else 'NA'

    def _text(val):
        return val[0] if isinstance(val, tuple) else val

    df['link'] = df['Name'].apply(_href)
    df['id_type'] = df['link'].apply(
        lambda v: 'baseball_reference_minor_league_id' if 'register' in v
        else 'baseball_reference_id' if 'players' in v
        else 'NA'
    )
    df['player_id'] = df['link'].apply(
        lambda v: v.split('=')[-1] if 'register' in v
        else v.split('/')[-1].split('.')[0] if 'players' in v
        else 'NA'
    )
    df['notes'] = df['Name'].apply(
        lambda v: v[0].split('(')[1].split(')')[0]
        if isinstance(v, tuple) and v[1] is None and isinstance(v[0], str) and '(' in v[0]
        else 'NA'
    )
    df['team_id'] = df['Tm'].apply(
        lambda v: v[1].split('team_ID=')[-1].split('&year_ID=')[0]
        if isinstance(v, tuple) and v[1] else 'NA'
    )
    for col in df.columns:
        if col not in ('link', 'id_type', 'player_id', 'notes', 'team_id'):
            df[col] = df[col].apply(_text)
    return df


def _fetch_round(draft_type: str, year: int, round_num: int) -> pl.DataFrame | None:
    url = BASE_URL.format(year=year, round=round_num, draft_type=draft_type)
    logging.info(f'Fetching {draft_type} {year} round {round_num}')
    response = requests.get(url, headers={'User-Agent': 'Test'}, timeout=30)
    response.raise_for_status()

    logging.info('Parsing HTML tables')
    tables = pd.read_html(StringIO(response.text), extract_links='body')
    if not tables:
        return None

    logging.info(f'Found {len(tables)} tables')
    df = pd.concat(tables, ignore_index=True)
    df = _clean_with_links(df)
    df = df.rename(columns=COLUMN_RENAMES)

    # Drop non-data rows (BBRef repeats the header row within the table body)
    logging.info(f'Filtering rows for year {year}')
    df = df[pd.to_numeric(df['Year'], errors='coerce') == year]
    df = df.dropna(subset=list(PRIMARY_KEYS - {'draft_type'}))
    if df.empty:
        return None

    return pl.from_pandas(df.astype(str))


def _fetch_round_with_retry(draft_type: str, year: int, round_num: int) -> pl.DataFrame | None:
    last_exc = None
    for attempt in range(len(RETRY_BACKOFF) + 1):
        if attempt > 0:
            wait = RETRY_BACKOFF[attempt - 1]
            logging.warning(f'Retrying {draft_type} {year} round {round_num} in {wait}s (attempt {attempt + 1})')
            time.sleep(wait)
        try:
            return _fetch_round(draft_type, year, round_num)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_exc = e
            logging.warning(f'Attempt {attempt + 1} failed for {draft_type} {year} round {round_num}: {e}')
    raise last_exc


def _to_arrow(df: pl.DataFrame) -> pa.Table:
    table = df.to_arrow()
    logging.info('Converting DataFrame to Arrow Table')
    schema = pa.schema([
        f.with_type(pa.utf8()).with_nullable(f.name not in PRIMARY_KEYS)
        if f.type == pa.large_utf8()
        else f
        for f in table.schema
    ])
    return table.cast(schema)


@dlt.resource(
    name='draft_results',
    write_disposition='merge',
    primary_key=['Year', 'draft_type', 'Rnd', 'RdPck'],
)
def draft_results(
    start_year: int,
    end_year: int,
    draft_types: tuple[DraftType, ...] = (DraftType.JUNREG,),
    update: bool = False,
    rounds_filter: dict[str, list[int]] | None = None,
    failed_rounds=None,
) -> Iterator:
    state = dlt.current.resource_state()

    types_to_process = [DraftType(t) for t in rounds_filter] if rounds_filter else list(draft_types)

    for draft_type in types_to_process:
        type_info = DRAFT_YEARS[draft_type.value]
        type_start = type_info['start_year']
        type_end = type_info['end_year']
        from_year = max(start_year, type_start) if update else max(state.get(draft_type.value, type_start), start_year)

        for year in range(from_year, min(end_year, type_end) + 1):

            # 1971 junsec data is not reliably fetchable; use helper CSVs instead
            if draft_type == DraftType.JUNSEC and year == 1971:
                dfs = [
                    pl.read_csv(os.path.join(HELPERS_PATH, filename), infer_schema_length=0)
                    for filename in [
                        'baseball_reference_draft_results_junsec_1971.csv',
                        'baseball_reference_draft_results_junsecd_1971.csv',
                    ]
                ]
                df = pl.concat(dfs).with_columns(pl.lit(draft_type.value).alias('draft_type'))
                yield _to_arrow(df)
                state[draft_type.value] = year
                continue

            rounds = _rounds_for_year(draft_type.value, year)
            if rounds == 0:
                continue

            round_list = rounds_filter[draft_type.value] if rounds_filter else list(range(1, rounds + 1))

            year_frames = []
            for round_num in round_list:
                try:
                    df = _fetch_round_with_retry(draft_type.value, year, round_num)
                    if df is not None:
                        year_frames.append(df)
                except Exception as e:
                    if failed_rounds is not None:
                        failed_rounds.append((draft_type.value, year, round_num))
                    logging.warning(f'Could not load {draft_type.value} {year} round {round_num}: {e}')
                time.sleep(5)

            if year_frames:
                logging.info(f'Combining {len(year_frames)} rounds for {draft_type.value} {year}')
                combined = pl.concat(year_frames, how='diagonal').with_columns(
                    pl.lit(draft_type.value).alias('draft_type')
                )
                yield _to_arrow(combined)
            state[draft_type.value] = year


@dlt.source
def baseball_reference_draft(
    start_year: int,
    end_year: int,
    draft_types: tuple[DraftType, ...] = (DraftType.JUNREG,),
    update: bool = False,
    rounds_filter: dict[str, list[int]] | None = None,
    failed_rounds=None,
):
    yield draft_results(start_year, end_year, draft_types, update, rounds_filter, failed_rounds)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=1965)
    parser.add_argument('--end', type=int, default=2025)
    parser.add_argument('--year', type=int)
    parser.add_argument('--draft-types', nargs='+', default=None,
                        choices=[d.value for d in DraftType])
    parser.add_argument('--all-draft-types', action='store_true')
    parser.add_argument('--rounds', type=str, default=None)
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--full-refresh', action='store_true')
    args = parser.parse_args()

    start_year = args.year if args.year else args.start
    end_year = args.year if args.year else args.end

    rounds_filter = json.loads(args.rounds) if args.rounds else None

    if rounds_filter is not None:
        draft_types = tuple(DraftType(t) for t in rounds_filter)
    elif args.draft_types is not None:
        draft_types = tuple(DraftType(t) for t in args.draft_types)
    elif args.all_draft_types:
        draft_types = tuple(DraftType)
    else:
        draft_types = (DraftType.JUNREG,)

    pipeline = dlt.pipeline(
        pipeline_name='baseball_reference_draft',
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name='baseball_reference_draft',
    )

    failed_rounds = []

    source = baseball_reference_draft(
        start_year=start_year,
        end_year=end_year,
        draft_types=draft_types,
        update=args.update,
        rounds_filter=rounds_filter,
        failed_rounds=failed_rounds,
    )

    if args.full_refresh:
        with pipeline.destination_client() as client:
            try:
                client.drop_storage()
            except DatabaseUndefinedRelation:
                pass
        pipeline.drop()

    load_info = pipeline.run(source)
    print(load_info)

    if failed_rounds:
        by_year = defaultdict(lambda: defaultdict(list))
        for dt, year, round_num in failed_rounds:
            by_year[year][dt].append(round_num)

        print('\nFailed rounds:')
        for year, type_rounds in sorted(by_year.items()):
            for dt, rnds in sorted(type_rounds.items()):
                print(f'  {dt} {year}: {sorted(rnds)}')

        print('\nTo retry:')
        for year, type_rounds in sorted(by_year.items()):
            rounds_json = json.dumps({dt: sorted(rnds) for dt, rnds in sorted(type_rounds.items())})
            print(f"  python {sys.argv[0]} --year {year} --rounds '{rounds_json}' --update")
