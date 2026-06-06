import dlt

from typing import Iterator

from loaders.cli import add_resources_arg, add_season_args, resolve_seasons, run_loader, validate_season_args
from loaders.dlt_utils import make_pipeline
from loaders.statcast._common import BASE_URL, TODAY, run_years

STATCAST_START_YEAR = 2015

ARSENAL_TYPES = ('avg_speed', 'n', 'avg_spin', 'avg_break_x', 'avg_break_z', 'avg_vert_break')
PITCH_TYPES = ('FF', 'SI', 'FC', 'SL', 'CH', 'CU', 'FS', 'KN', 'ST', 'SV')
ACTIVE_SPIN_TYPES = ('spin-based', 'movement-based')


@dlt.resource(name='exit_velo_barrels', write_disposition='merge', primary_key=['player_id', 'year'])
def exit_velo_barrels(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'exit_velo_barrels', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/statcast?type=pitcher&year={y}&position=&team=&min=1&csv=true')],
        update,
    )


@dlt.resource(name='expected_stats', write_disposition='merge', primary_key=['player_id', 'year'])
def expected_stats(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'expected_stats', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/expected_statistics?type=pitcher&year={y}&position=&team=&min=1&csv=true')],
        update,
    )


@dlt.resource(name='percentile_ranks', write_disposition='merge', primary_key=['player_id', 'year'])
def percentile_ranks(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'percentile_ranks', {'player_id', 'year'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/percentile-rankings?type=pitcher&year={y}&position=&team=&csv=true')],
        update,
    )


# pitch_arsenals columns pivot based on arsenal_type (avg_speed / avg_spin / avg_break_x / etc.)
# so each type is stored as a separate arsenal_type-labeled row set sharing (pitcher, year).
@dlt.resource(name='pitch_arsenals', write_disposition='merge', primary_key=['pitcher', 'year', 'arsenal_type'])
def pitch_arsenals(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for at in ARSENAL_TYPES:
            yield ({'arsenal_type': at},
                   f'{BASE_URL}/pitch-arsenals?year={y}&min=100&type={at}&hand=&csv=true')
    yield from run_years('pitch_arsenals', {'pitcher', 'year', 'arsenal_type'}, start_year, end_year, iter_year, update)


@dlt.resource(name='pitch_arsenal_stats', write_disposition='merge', primary_key=['player_id', 'year', 'pitch_type'])
def pitch_arsenal_stats(start_year: int, end_year: int, update: bool = False) -> Iterator:
    yield from run_years(
        'pitch_arsenal_stats', {'player_id', 'year', 'pitch_type'}, start_year, end_year,
        lambda y: [({}, f'{BASE_URL}/pitch-arsenal-stats?type=pitcher&pitchType=&year={y}&team=&min=1&minPitches=1&csv=true')],
        update,
    )


@dlt.resource(name='pitch_movement', write_disposition='merge', primary_key=['pitcher_id', 'year', 'pitch_type'])
def pitch_movement(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for pt in PITCH_TYPES:
            yield ({}, (
                f'{BASE_URL}/pitch-movement?year={y}&team=&min=50'
                f'&pitch_type={pt}&hand=&x=pitcher_break_x_hidden&z=pitcher_break_z_hidden&csv=true'
            ))
    yield from run_years('pitch_movement', {'pitcher_id', 'year', 'pitch_type'}, start_year, end_year, iter_year, update)


# active_spin columns pivot by spin_type (spin-based vs movement-based); label rows so both can coexist.
@dlt.resource(name='active_spin', write_disposition='merge', primary_key=['entity_id', 'year', 'spin_type'])
def active_spin(start_year: int, end_year: int, update: bool = False) -> Iterator:
    def iter_year(y):
        for st in ACTIVE_SPIN_TYPES:
            yield ({'spin_type': st},
                   f'{BASE_URL}/active-spin?year={y}_{st}&min=50&hand=&csv=true')
    yield from run_years('active_spin', {'entity_id', 'year', 'spin_type'}, start_year, end_year, iter_year, update)


# bat_tracking from pitcher's perspective; column 'id' holds pitcher id. Data begins 2023.
# game_type accepts 'Regular', 'Playoff', or 'all' (empty Savant filter — all game types).
@dlt.resource(name='bat_tracking', write_disposition='merge', primary_key=['id', 'year'])
def bat_tracking(start_year: int, end_year: int, update: bool = False, game_type: str = 'Regular') -> Iterator:
    gt = '' if game_type == 'all' else game_type
    yield from run_years(
        'bat_tracking', {'id', 'year'}, start_year, end_year,
        lambda y: [({}, (
            f'{BASE_URL}/bat-tracking?'
            f'gameType={gt}&minSwings=1&minGroupSwings=1&seasonStart={y}&seasonEnd={y}&type=pitcher&csv=true'
        ))],
        update,
    )


@dlt.source
def statcast_pitching_leaderboards(start_year: int, end_year: int, update: bool = False, game_type: str = 'Regular'):
    yield exit_velo_barrels(start_year, end_year, update)
    yield expected_stats(start_year, end_year, update)
    yield percentile_ranks(start_year, end_year, update)
    yield pitch_arsenals(start_year, end_year, update)
    yield pitch_arsenal_stats(start_year, end_year, update)
    yield pitch_movement(start_year, end_year, update)
    yield active_spin(start_year, end_year, update)
    yield bat_tracking(start_year, end_year, update, game_type)


def register(subparsers):
    parser = subparsers.add_parser('statcast-pitching', help='Statcast pitching leaderboards')
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

    pipeline = make_pipeline('statcast_pitching_leaderboards')

    source = statcast_pitching_leaderboards(start_year=start_year, end_year=end_year, update=args.update, game_type=args.game_type)

    run_loader(pipeline, source, args)
