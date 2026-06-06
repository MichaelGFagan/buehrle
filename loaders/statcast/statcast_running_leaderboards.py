import dlt

from typing import Iterator

from loaders.cli import add_resources_arg, add_season_args, resolve_seasons, run_loader, validate_season_args
from loaders.dlt_utils import make_pipeline
from loaders.statcast._common import BASE_URL, TODAY, run_years

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


def register(subparsers):
    parser = subparsers.add_parser('statcast-running', help='Statcast running leaderboards')
    add_season_args(parser, STATCAST_START_YEAR)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    add_resources_arg(parser)
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    validate_season_args(parser, args)
    start_year, end_year = resolve_seasons(args, STATCAST_START_YEAR)

    pipeline = make_pipeline('statcast_running_leaderboards')

    source = statcast_running_leaderboards(start_year=start_year, end_year=end_year, update=args.update)

    run_loader(pipeline, source, args)
