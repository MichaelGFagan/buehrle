import dlt

from typing import Iterator

from loaders.cli import add_resources_arg, add_season_args, apply_resources, resolve_seasons, validate_season_args
from loaders.statcast._common import BASE_URL, SAVANT_HOST, TODAY, handle_full_refresh, make_pipeline, run_years

STATCAST_START_YEAR = 2016  # OAA leaderboards begin 2016

FIELDING_POSITIONS = ('2', '3', '4', '5', '6', '7', '8', '9', 'OF')


@dlt.resource(name='outs_above_average', write_disposition='merge', primary_key=['player_id', 'year', 'position'])
def outs_above_average(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for pos in FIELDING_POSITIONS:
            if pos == 'OF':
                continue
            url = (
                f'{BASE_URL}/outs_above_average'
                f'?type=Fielder&startYear={y}&endYear={y}&split=no&team=&range=year&min=10&pos={pos}&roles=&viz=hide&csv=true'
            )
            yield ({'position': pos}, url)
    yield from run_years('outs_above_average', {'player_id', 'year', 'position'}, start_year, end_year, iter_year, update)


@dlt.resource(name='fielding_run_value', write_disposition='merge', primary_key=['id', 'year', 'position'])
def fielding_run_value(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for pos in FIELDING_POSITIONS:
            url = f'{BASE_URL}/fielding-run-value?type=fielder&seasonStart={y}&seasonEnd={y}&position={pos}&minInnings=0.1&minResults=0.1&csv=true'
            yield ({'position': pos}, url)
    yield from run_years('fielding_run_value', {'id', 'year', 'position'}, start_year, end_year, iter_year, update)


@dlt.resource(name='outfield_directional_oaa', write_disposition='merge', primary_key=['player_id', 'year'])
def outfield_directional_oaa(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'outfield_directional_oaa', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{SAVANT_HOST}/directional_outs_above_average?year={y}&min=0&team=&csv=true')],
        update,
    )


@dlt.resource(name='outfield_catch_prob', write_disposition='merge', primary_key=['player_id', 'year'])
def outfield_catch_prob(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'outfield_catch_prob', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/catch_probability?type=player&min=0&year={y}&total=&csv=true')],
        update,
    )


@dlt.resource(name='outfielder_jump', write_disposition='merge', primary_key=['resp_fielder_id', 'year'])
def outfielder_jump(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'outfielder_jump', {'resp_fielder_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/outfield_jump?year={y}&min=10&csv=true')],
        update,
    )


@dlt.resource(name='catcher_poptime', write_disposition='merge', primary_key=['entity_id', 'year'])
def catcher_poptime(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'catcher_poptime', {'entity_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/poptime?year={y}&team=&min2b=1&min3b=0&csv=true')],
        update,
    )


@dlt.resource(name='catcher_framing', write_disposition='merge', primary_key=['id', 'year'])
def catcher_framing(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'catcher_framing', {'id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/catcher-framing?type=catcher&seasonStart={y}&seasonEnd={y}&team=&min=0&sortColumn=rv_tot&sortDirection=desc&csv=true')],
        update,
    )


@dlt.source
def statcast_fielding_leaderboards(start_year: int, end_year: int, update: bool = False):
    yield outs_above_average(start_year, end_year, update)
    yield fielding_run_value(start_year, end_year, update)
    yield outfield_directional_oaa(start_year, end_year, update)
    yield outfield_catch_prob(start_year, end_year, update)
    yield outfielder_jump(start_year, end_year, update)
    yield catcher_poptime(start_year, end_year, update)
    yield catcher_framing(start_year, end_year, update)


def register(subparsers):
    parser = subparsers.add_parser('statcast-fielding', help='Statcast fielding leaderboards')
    add_season_args(parser, STATCAST_START_YEAR)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    add_resources_arg(parser)
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    validate_season_args(parser, args)
    start_year, end_year = resolve_seasons(args, STATCAST_START_YEAR)

    pipeline = make_pipeline('statcast_fielding_leaderboards')

    source = statcast_fielding_leaderboards(start_year=start_year, end_year=end_year, update=args.update)
    source = apply_resources(source, args)

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
