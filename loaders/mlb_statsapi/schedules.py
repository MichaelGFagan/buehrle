"""
Loader for the MLB Stats API /v1/schedule endpoint.

One HTTP call per season (or one call for a date / date range) populates three tables:
  - schedules                    : one row per game
  - schedules_linescore_innings  : one row per (game, inning)
  - schedules_officials          : one row per (game, official_type)

See docs/mlb_statsapi.md for endpoint shape, hydrations, and the deferred-sidecar list.
"""

import datetime
import logging
from typing import Iterator

import dlt
import polars as pl
import pyarrow as pa
import requests

from loaders.cli import add_date_args, add_resources_arg, add_season_args, apply_resources, resolve_scope, validate_scope_args
from loaders.dlt_utils import handle_full_refresh, make_pipeline, to_arrow

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

BASE_URL = 'https://statsapi.mlb.com/api/v1/schedule'
HYDRATE = 'decisions,gameInfo,weather,flags,seriesStatus,linescore,officials'
SPORT_ID = 1
TIMEOUT = 60
# Earliest season per /v1/seasons/all (see loaders/mlb_statsapi/samples/seasons_all.json).
EARLIEST_SEASON = 1876


def _fetch(params: dict) -> dict:
    full_params = {'sportId': SPORT_ID, 'hydrate': HYDRATE, **params}
    logging.info(f'GET {BASE_URL} {full_params}')
    response = requests.get(BASE_URL, params=full_params, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def _fetch_payloads(seasons: list[int] | None,
                    date_range: tuple[datetime.date, datetime.date] | None) -> Iterator[dict]:
    if seasons:
        for season in seasons:
            yield _fetch({'season': str(season)})
    elif date_range:
        start, end = date_range
        if start == end:
            yield _fetch({'date': start.isoformat()})
        else:
            yield _fetch({'startDate': start.isoformat(), 'endDate': end.isoformat()})


def _games_from_payloads(payloads: list[dict]) -> list[dict]:
    return [g for p in payloads for d in p.get('dates', []) for g in d.get('games', [])]


def _flatten_game(game: dict) -> dict:
    teams = game.get('teams') or {}
    home = teams.get('home') or {}
    away = teams.get('away') or {}
    home_team = home.get('team') or {}
    away_team = away.get('team') or {}
    home_record = home.get('leagueRecord') or {}
    away_record = away.get('leagueRecord') or {}

    status = game.get('status') or {}
    venue = game.get('venue') or {}
    content = game.get('content') or {}

    decisions = game.get('decisions') or {}
    winner = decisions.get('winner') or {}
    loser = decisions.get('loser') or {}
    save = decisions.get('save') or {}

    game_info = game.get('gameInfo') or {}
    weather = game.get('weather') or {}
    flags = game.get('flags') or {}

    series = game.get('seriesStatus') or {}
    series_winner = series.get('winningTeam') or {}
    series_loser = series.get('losingTeam') or {}

    linescore = game.get('linescore') or {}
    ls_teams = linescore.get('teams') or {}
    ls_home = ls_teams.get('home') or {}
    ls_away = ls_teams.get('away') or {}

    return {
        'game_pk': game.get('gamePk'),
        'game_guid': game.get('gameGuid'),
        'link': game.get('link'),
        'season': game.get('season'),
        'season_display': game.get('seasonDisplay'),
        'game_type': game.get('gameType'),
        'game_date': game.get('gameDate'),
        'official_date': game.get('officialDate'),
        'day_night': game.get('dayNight'),
        'scheduled_innings': game.get('scheduledInnings'),
        'game_number': game.get('gameNumber'),
        'double_header': game.get('doubleHeader'),
        'games_in_series': game.get('gamesInSeries'),
        'series_game_number': game.get('seriesGameNumber'),
        'series_description': game.get('seriesDescription'),
        'tiebreaker': game.get('tiebreaker'),
        'if_necessary': game.get('ifNecessary'),
        'if_necessary_description': game.get('ifNecessaryDescription'),
        'gameday_type': game.get('gamedayType'),
        'record_source': game.get('recordSource'),
        'calendar_event_id': game.get('calendarEventID'),
        'public_facing': game.get('publicFacing'),
        'reverse_home_away_status': game.get('reverseHomeAwayStatus'),
        'inning_break_length': game.get('inningBreakLength'),
        'is_tie': game.get('isTie'),

        'status_abstract': status.get('abstractGameState'),
        'status_abstract_code': status.get('abstractGameCode'),
        'status_coded': status.get('codedGameState'),
        'status_detailed': status.get('detailedState'),
        'status_status_code': status.get('statusCode'),
        'status_start_time_tbd': status.get('startTimeTBD'),

        'home_team_id': home_team.get('id'),
        'home_team_name': home_team.get('name'),
        'away_team_id': away_team.get('id'),
        'away_team_name': away_team.get('name'),
        'home_score': home.get('score'),
        'away_score': away.get('score'),
        'home_is_winner': home.get('isWinner'),
        'away_is_winner': away.get('isWinner'),
        'home_split_squad': home.get('splitSquad'),
        'away_split_squad': away.get('splitSquad'),
        'home_series_number': home.get('seriesNumber'),
        'away_series_number': away.get('seriesNumber'),
        'home_record_wins': home_record.get('wins'),
        'home_record_losses': home_record.get('losses'),
        'home_record_pct': home_record.get('pct'),
        'away_record_wins': away_record.get('wins'),
        'away_record_losses': away_record.get('losses'),
        'away_record_pct': away_record.get('pct'),

        'venue_id': venue.get('id'),
        'venue_name': venue.get('name'),
        'content_link': content.get('link'),

        'winning_pitcher_id': winner.get('id'),
        'winning_pitcher_name': winner.get('fullName'),
        'losing_pitcher_id': loser.get('id'),
        'losing_pitcher_name': loser.get('fullName'),
        'save_pitcher_id': save.get('id'),
        'save_pitcher_name': save.get('fullName'),

        'attendance': game_info.get('attendance'),
        'first_pitch': game_info.get('firstPitch'),
        'game_duration_minutes': game_info.get('gameDurationMinutes'),

        'weather_condition': weather.get('condition'),
        'weather_temp': weather.get('temp'),
        'weather_wind': weather.get('wind'),

        'no_hitter': flags.get('noHitter'),
        'perfect_game': flags.get('perfectGame'),
        'home_team_no_hitter': flags.get('homeTeamNoHitter'),
        'home_team_perfect_game': flags.get('homeTeamPerfectGame'),
        'away_team_no_hitter': flags.get('awayTeamNoHitter'),
        'away_team_perfect_game': flags.get('awayTeamPerfectGame'),

        'series_status_result': series.get('result'),
        'series_status_description': series.get('description'),
        'series_status_short_description': series.get('shortDescription'),
        'series_status_wins': series.get('wins'),
        'series_status_losses': series.get('losses'),
        'series_status_total_games': series.get('totalGames'),
        'series_status_game_number': series.get('gameNumber'),
        'series_status_is_over': series.get('isOver'),
        'series_status_is_tied': series.get('isTied'),
        'series_status_winning_team_id': series_winner.get('id'),
        'series_status_losing_team_id': series_loser.get('id'),

        'current_inning': linescore.get('currentInning'),
        'inning_state': linescore.get('inningState'),
        'is_top_inning': linescore.get('isTopInning'),
        'balls': linescore.get('balls'),
        'strikes': linescore.get('strikes'),
        'outs': linescore.get('outs'),
        'home_line_runs': ls_home.get('runs'),
        'home_line_hits': ls_home.get('hits'),
        'home_line_errors': ls_home.get('errors'),
        'home_line_lob': ls_home.get('leftOnBase'),
        'away_line_runs': ls_away.get('runs'),
        'away_line_hits': ls_away.get('hits'),
        'away_line_errors': ls_away.get('errors'),
        'away_line_lob': ls_away.get('leftOnBase'),
    }


def _innings_rows(games: list[dict]) -> Iterator[dict]:
    for game in games:
        game_pk = game.get('gamePk')
        for inning in (game.get('linescore') or {}).get('innings', []):
            home = inning.get('home') or {}
            away = inning.get('away') or {}
            yield {
                'game_pk': game_pk,
                'inning_num': inning.get('num'),
                'inning_ordinal': inning.get('ordinalNum'),
                'home_runs': home.get('runs'),
                'home_hits': home.get('hits'),
                'home_errors': home.get('errors'),
                'home_left_on_base': home.get('leftOnBase'),
                'away_runs': away.get('runs'),
                'away_hits': away.get('hits'),
                'away_errors': away.get('errors'),
                'away_left_on_base': away.get('leftOnBase'),
            }


def _officials_rows(games: list[dict]) -> Iterator[dict]:
    for game in games:
        game_pk = game.get('gamePk')
        for entry in game.get('officials') or []:
            official = entry.get('official') or {}
            yield {
                'game_pk': game_pk,
                'official_type': entry.get('officialType'),
                'official_id': official.get('id'),
                'official_full_name': official.get('fullName'),
            }


def _to_arrow(rows: list[dict], primary_keys: set[str]) -> pa.Table:
    df = pl.from_dicts(rows, infer_schema_length=None)
    return to_arrow(df, primary_keys=primary_keys)


@dlt.source
def schedules_source(seasons: list[int] | None = None,
                     date_range: tuple[datetime.date, datetime.date] | None = None):
    payloads = list(_fetch_payloads(seasons, date_range))
    games = _games_from_payloads(payloads)
    logging.info(f'Fetched {len(games)} games across {len(payloads)} API call(s)')

    @dlt.resource(name='schedules', primary_key='game_pk', write_disposition='merge')
    def schedules() -> Iterator[pa.Table]:
        if not games:
            return
        yield _to_arrow([_flatten_game(g) for g in games], primary_keys={'game_pk'})

    @dlt.resource(name='schedules_linescore_innings',
                  primary_key=['game_pk', 'inning_num'],
                  write_disposition='merge')
    def schedules_linescore_innings() -> Iterator[pa.Table]:
        rows = list(_innings_rows(games))
        if not rows:
            return
        yield _to_arrow(rows, primary_keys={'game_pk', 'inning_num'})

    @dlt.resource(name='schedules_officials',
                  primary_key=['game_pk', 'official_type'],
                  write_disposition='merge')
    def schedules_officials() -> Iterator[pa.Table]:
        rows = list(_officials_rows(games))
        if not rows:
            return
        yield _to_arrow(rows, primary_keys={'game_pk', 'official_type'})

    return schedules, schedules_linescore_innings, schedules_officials


def register(subparsers):
    parser = subparsers.add_parser('mlb-statsapi-schedules', help='MLB Stats API: schedules endpoint')
    add_season_args(parser, EARLIEST_SEASON)
    add_date_args(parser)
    parser.add_argument('--full-refresh', action='store_true')
    add_resources_arg(parser)
    parser.set_defaults(func=lambda args: main(parser, args))


def main(parser, args):
    validate_scope_args(parser, args)
    scope = resolve_scope(args, EARLIEST_SEASON)

    pipeline = make_pipeline('mlb_statsapi_schedules')
    source = schedules_source(seasons=scope['seasons'], date_range=scope['dates'])
    source = apply_resources(source, args)

    if args.full_refresh:
        handle_full_refresh(pipeline)

    load_info = pipeline.run(source)
    print(load_info)
