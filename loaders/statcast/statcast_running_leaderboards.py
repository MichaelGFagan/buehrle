import argparse
import dlt

from typing import Iterator

from loaders.statcast._common import BASE_URL, TODAY, handle_full_refresh, make_pipeline, run_years

STATCAST_START_YEAR = 2015

RUNNING_SPLIT_TYPES = ('raw', 'percent')


@dlt.resource(name='sprint_speed', write_disposition='merge', primary_key=['player_id', 'year'])
def sprint_speed(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'sprint_speed', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/sprint_speed?year={y}&position=&team=&min=0&csv=true')],
        update,
    )


# running_splits returns the same columns for raw vs percent but different values;
# split_type discriminates the row sets in a single table.
@dlt.resource(name='running_splits', write_disposition='merge', primary_key=['player_id', 'year', 'split_type'])
def running_splits(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for st in RUNNING_SPLIT_TYPES:
            yield ({'split_type': st},
                   f'{BASE_URL}/running_splits?type={st}&bats=&year={y}&position=&team=&min=5&csv=true')
    yield from run_years('running_splits', {'player_id', 'year', 'split_type'}, start_year, end_year, iter_year, update)


@dlt.source
def statcast_running_leaderboards(start_year: int, end_year: int, update: bool = False):
    yield sprint_speed(start_year, end_year, update)
    yield running_splits(start_year, end_year, update)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=STATCAST_START_YEAR)
    parser.add_argument('--end', type=int, default=TODAY.year)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--resources', nargs='+', default=None)
    args = parser.parse_args()

    pipeline = make_pipeline('statcast_running_leaderboards')

    source = statcast_running_leaderboards(start_year=args.start, end_year=args.end, update=args.update)
    if args.resources:
        source = source.with_resources(*args.resources)

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
