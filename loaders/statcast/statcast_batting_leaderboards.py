import dlt

from typing import Iterator

from loaders.cli import add_resources_arg, add_season_args, apply_resources, resolve_seasons, validate_season_args
from loaders.statcast._common import BASE_URL, TODAY, handle_full_refresh, make_pipeline, run_years

STATCAST_START_YEAR = 2015


@dlt.resource(name='exit_velo_barrels', write_disposition='merge', primary_key=['player_id', 'year'])
def exit_velo_barrels(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'exit_velo_barrels', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/statcast?type=batter&year={y}&position=&team=&min=1&csv=true')],
        update,
    )


@dlt.resource(name='expected_stats', write_disposition='merge', primary_key=['player_id', 'year'])
def expected_stats(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'expected_stats', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/expected_statistics?type=batter&year={y}&position=&team=&min=1&csv=true')],
        update,
    )


@dlt.resource(name='percentile_ranks', write_disposition='merge', primary_key=['player_id', 'year'])
def percentile_ranks(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'percentile_ranks', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/percentile-rankings?type=batter&year={y}&position=&team=&csv=true')],
        update,
    )


@dlt.resource(name='pitch_arsenal_stats', write_disposition='merge', primary_key=['player_id', 'year', 'pitch_type'])
def pitch_arsenal_stats(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'pitch_arsenal_stats', {'player_id', 'year', 'pitch_type'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/pitch-arsenal-stats?type=batter&pitchType=&year={y}&team=&min=1&csv=true')],
        update,
    )


# bat_tracking returns column 'id' (not 'player_id'); Savant began publishing this in 2023.
# game_type accepts 'Regular', 'Playoff', or 'all' (empty Savant filter — all game types).
@dlt.resource(name='bat_tracking', write_disposition='merge', primary_key=['id', 'year'])
def bat_tracking(start_year: int, end_year: int, update: bool = False, game_type: str = 'Regular') -> Iterator:
    gt = '' if game_type == 'all' else game_type
    yield from run_years(
        'bat_tracking', {'id', 'year'}, start_year, end_year,
        lambda y: [({}, (
            f'{BASE_URL}/bat-tracking?'
            f'gameType={gt}&minSwings=1&minGroupSwings=1&seasonStart={y}&seasonEnd={y}&type=batter&csv=true'
        ))],
        update,
    )


@dlt.source
def statcast_batting_leaderboards(start_year: int, end_year: int, update: bool = False, game_type: str = 'Regular'):
    yield exit_velo_barrels(start_year, end_year, update)
    yield expected_stats(start_year, end_year, update)
    yield percentile_ranks(start_year, end_year, update)
    yield pitch_arsenal_stats(start_year, end_year, update)
    yield bat_tracking(start_year, end_year, update, game_type)


def register(subparsers):
    parser = subparsers.add_parser('statcast-batting', help='Statcast batting leaderboards')
    add_season_args(parser, STATCAST_START_YEAR)
    parser.add_argument('--full-refresh', action='store_true')
    parser.add_argument('--update', action='store_true')
    add_resources_arg(parser)
    parser.add_argument('--game-type', choices=['Regular', 'Playoff', 'all'], default='Regular',
                        help="Savant gameType filter for bat_tracking (default 'Regular')")
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    validate_season_args(parser, args)
    start_year, end_year = resolve_seasons(args, STATCAST_START_YEAR)

    pipeline = make_pipeline('statcast_batting_leaderboards')

    source = statcast_batting_leaderboards(start_year=start_year, end_year=end_year, update=args.update, game_type=args.game_type)
    source = apply_resources(source, args)

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
