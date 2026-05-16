import argparse
import datetime
import runpy
import sys
from unittest.mock import MagicMock

import duckdb
import pytest

from loaders.mlb_statsapi import schedules


# ---------- fixtures ----------

def _hydrated_game(game_pk=824522, away_score=10, home_score=0):
    return {
        'gamePk': game_pk,
        'gameGuid': '218d8b9a-2b64-4212-82d4-9c6eaa1157d0',
        'link': f'/api/v1.1/game/{game_pk}/feed/live',
        'gameType': 'R',
        'season': '2026',
        'seasonDisplay': '2026',
        'gameDate': '2026-05-08T22:10:00Z',
        'officialDate': '2026-05-08',
        'dayNight': 'night',
        'scheduledInnings': 9,
        'gameNumber': 1,
        'doubleHeader': 'N',
        'gamesInSeries': 3,
        'seriesGameNumber': 1,
        'seriesDescription': 'Regular Season',
        'tiebreaker': 'N',
        'ifNecessary': 'N',
        'ifNecessaryDescription': 'Normal Game',
        'gamedayType': 'P',
        'recordSource': 'S',
        'calendarEventID': f'14-{game_pk}-2026-05-08',
        'publicFacing': True,
        'reverseHomeAwayStatus': False,
        'inningBreakLength': 120,
        'isTie': False,
        'status': {
            'abstractGameState': 'Final',
            'abstractGameCode': 'F',
            'codedGameState': 'F',
            'detailedState': 'Final',
            'statusCode': 'F',
            'startTimeTBD': False,
        },
        'teams': {
            'away': {
                'team': {'id': 117, 'name': 'Houston Astros'},
                'leagueRecord': {'wins': 16, 'losses': 23, 'pct': '.410'},
                'score': away_score,
                'isWinner': away_score > home_score,
                'splitSquad': False,
                'seriesNumber': 13,
            },
            'home': {
                'team': {'id': 113, 'name': 'Cincinnati Reds'},
                'leagueRecord': {'wins': 20, 'losses': 19, 'pct': '.513'},
                'score': home_score,
                'isWinner': home_score > away_score,
                'splitSquad': False,
                'seriesNumber': 13,
            },
        },
        'venue': {'id': 2602, 'name': 'Great American Ball Park'},
        'content': {'link': f'/api/v1/game/{game_pk}/content'},
        'decisions': {
            'winner': {'id': 691023, 'fullName': 'Spencer Arrighetti'},
            'loser': {'id': 666157, 'fullName': 'Nick Lodolo'},
            'save': {'id': 656232, 'fullName': 'Bryan Abreu'},
        },
        'gameInfo': {
            'attendance': 38432,
            'firstPitch': '2026-05-08T22:14:00Z',
            'gameDurationMinutes': 165,
        },
        'weather': {'condition': 'Cloudy', 'temp': '72', 'wind': '8 mph, In From CF'},
        'flags': {
            'noHitter': False, 'perfectGame': False,
            'awayTeamNoHitter': False, 'awayTeamPerfectGame': False,
            'homeTeamNoHitter': False, 'homeTeamPerfectGame': False,
        },
        'seriesStatus': {
            'result': 'Astros lead 1-0', 'description': 'Series Tied',
            'shortDescription': 'HOU 1-0', 'wins': 1, 'losses': 0,
            'totalGames': 3, 'gameNumber': 1, 'isOver': False, 'isTied': False,
            'winningTeam': {'id': 117}, 'losingTeam': {'id': 113},
        },
        'linescore': {
            'currentInning': 9, 'inningState': 'End', 'isTopInning': False,
            'balls': 0, 'strikes': 0, 'outs': 3,
            'innings': [
                {'num': 1, 'ordinalNum': '1st',
                 'home': {'runs': 0, 'hits': 1, 'errors': 0, 'leftOnBase': 1},
                 'away': {'runs': 0, 'hits': 1, 'errors': 0, 'leftOnBase': 0}},
                {'num': 2, 'ordinalNum': '2nd',
                 'home': {'runs': 0, 'hits': 0, 'errors': 0, 'leftOnBase': 0},
                 'away': {'runs': 2, 'hits': 2, 'errors': 0, 'leftOnBase': 0}},
            ],
            'teams': {
                'home': {'runs': home_score, 'hits': 5, 'errors': 0, 'leftOnBase': 6},
                'away': {'runs': away_score, 'hits': 13, 'errors': 0, 'leftOnBase': 5},
            },
        },
        'officials': [
            {'official': {'id': 605672, 'fullName': 'Jeremie Rehak'}, 'officialType': 'Home Plate'},
            {'official': {'id': 482663, 'fullName': 'David Rackley'}, 'officialType': 'First Base'},
            {'official': {'id': 644760, 'fullName': 'Adam Beck'}, 'officialType': 'Second Base'},
            {'official': {'id': 485856, 'fullName': 'Jonathan Parra'}, 'officialType': 'Third Base'},
        ],
    }


def _bare_game(game_pk=900001):
    """Game with only baseline (no-hydration) fields, like a Spring Training pre-game entry."""
    return {
        'gamePk': game_pk,
        'gameGuid': 'aaaa-bbbb',
        'link': f'/api/v1.1/game/{game_pk}/feed/live',
        'gameType': 'S',
        'season': '2026',
        'seasonDisplay': '2026',
        'gameDate': '2026-02-20T18:05:00Z',
        'officialDate': '2026-02-20',
        'dayNight': 'day',
        'scheduledInnings': 9,
        'gameNumber': 1,
        'doubleHeader': 'N',
        'gamesInSeries': 1,
        'seriesGameNumber': 1,
        'seriesDescription': 'Spring Training',
        'tiebreaker': 'N',
        'ifNecessary': 'N',
        'ifNecessaryDescription': 'Normal Game',
        'gamedayType': 'E',
        'recordSource': 'S',
        'calendarEventID': f'14-{game_pk}-2026-02-20',
        'publicFacing': True,
        'reverseHomeAwayStatus': False,
        'inningBreakLength': 120,
        'isTie': False,
        'status': {
            'abstractGameState': 'Preview', 'abstractGameCode': 'P',
            'codedGameState': 'S', 'detailedState': 'Scheduled',
            'statusCode': 'S', 'startTimeTBD': False,
        },
        'teams': {
            'away': {
                'team': {'id': 147, 'name': 'New York Yankees'},
                'leagueRecord': {'wins': 0, 'losses': 0, 'pct': '.000'},
                'splitSquad': False, 'seriesNumber': 1,
            },
            'home': {
                'team': {'id': 110, 'name': 'Baltimore Orioles'},
                'leagueRecord': {'wins': 0, 'losses': 0, 'pct': '.000'},
                'splitSquad': False, 'seriesNumber': 1,
            },
        },
        'venue': {'id': 2508, 'name': 'Ed Smith Stadium'},
        'content': {'link': f'/api/v1/game/{game_pk}/content'},
    }


def _payload(games):
    return {
        'totalItems': len(games),
        'totalGames': len(games),
        'dates': [{'date': g['officialDate'], 'games': [g]} for g in games],
    }


def _mock_response(payload):
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


# ---------- _flatten_game ----------

def test_flatten_game_with_full_hydration():
    row = schedules._flatten_game(_hydrated_game())

    # core
    assert row['game_pk'] == 824522
    assert row['game_type'] == 'R'
    assert row['official_date'] == '2026-05-08'
    assert row['status_detailed'] == 'Final'
    # teams
    assert row['away_team_id'] == 117
    assert row['home_team_id'] == 113
    assert row['away_score'] == 10
    assert row['home_score'] == 0
    assert row['away_is_winner'] is True
    assert row['home_is_winner'] is False
    assert row['away_record_wins'] == 16
    # decisions
    assert row['winning_pitcher_id'] == 691023
    assert row['winning_pitcher_name'] == 'Spencer Arrighetti'
    assert row['losing_pitcher_id'] == 666157
    assert row['save_pitcher_id'] == 656232
    # gameInfo
    assert row['attendance'] == 38432
    assert row['game_duration_minutes'] == 165
    # weather
    assert row['weather_condition'] == 'Cloudy'
    assert row['weather_temp'] == '72'
    # flags
    assert row['no_hitter'] is False
    # seriesStatus
    assert row['series_status_winning_team_id'] == 117
    assert row['series_status_losing_team_id'] == 113
    # linescore line totals
    assert row['home_line_runs'] == 0
    assert row['away_line_runs'] == 10
    assert row['away_line_hits'] == 13
    assert row['outs'] == 3


def test_flatten_game_with_missing_hydrations():
    """A bare game (e.g. Spring Training preview) shouldn't crash and should leave optional fields None."""
    row = schedules._flatten_game(_bare_game())

    assert row['game_pk'] == 900001
    assert row['status_detailed'] == 'Scheduled'
    # decisions absent
    assert row['winning_pitcher_id'] is None
    assert row['save_pitcher_name'] is None
    # gameInfo absent
    assert row['attendance'] is None
    # weather absent
    assert row['weather_condition'] is None
    # flags absent
    assert row['no_hitter'] is None
    # seriesStatus absent
    assert row['series_status_result'] is None
    assert row['series_status_winning_team_id'] is None
    # linescore absent
    assert row['current_inning'] is None
    assert row['home_line_runs'] is None
    # scores absent for unscheduled games
    assert row['home_score'] is None
    assert row['away_score'] is None


# ---------- _innings_rows ----------

def test_innings_rows_yields_one_per_inning():
    rows = list(schedules._innings_rows([_hydrated_game()]))

    assert len(rows) == 2
    assert rows[0] == {
        'game_pk': 824522, 'inning_num': 1, 'inning_ordinal': '1st',
        'home_runs': 0, 'home_hits': 1, 'home_errors': 0, 'home_left_on_base': 1,
        'away_runs': 0, 'away_hits': 1, 'away_errors': 0, 'away_left_on_base': 0,
    }
    assert rows[1]['inning_num'] == 2
    assert rows[1]['away_runs'] == 2


def test_innings_rows_skips_games_without_linescore():
    rows = list(schedules._innings_rows([_bare_game()]))
    assert rows == []


# ---------- _officials_rows ----------

def test_officials_rows_yields_one_per_umpire():
    rows = list(schedules._officials_rows([_hydrated_game()]))

    assert len(rows) == 4
    assert {r['official_type'] for r in rows} == {'Home Plate', 'First Base', 'Second Base', 'Third Base'}
    home_plate = next(r for r in rows if r['official_type'] == 'Home Plate')
    assert home_plate == {
        'game_pk': 824522, 'official_type': 'Home Plate',
        'official_id': 605672, 'official_full_name': 'Jeremie Rehak',
    }


def test_officials_rows_skips_games_without_officials():
    rows = list(schedules._officials_rows([_bare_game()]))
    assert rows == []


# ---------- _games_from_payloads ----------

def test_games_from_payloads_flattens_dates_and_games():
    g1, g2, g3 = _hydrated_game(1), _hydrated_game(2), _hydrated_game(3)
    payload = {
        'dates': [
            {'date': '2026-05-08', 'games': [g1, g2]},
            {'date': '2026-05-09', 'games': [g3]},
        ],
    }

    games = schedules._games_from_payloads([payload])

    assert [g['gamePk'] for g in games] == [1, 2, 3]


def test_games_from_payloads_handles_empty_payload():
    assert schedules._games_from_payloads([{'dates': []}]) == []


# ---------- _resolve_seasons_and_dates ----------

def _ns(**kwargs):
    defaults = dict(season=None, start_season=None, end_season=None,
                    date=None, start_date=None, end_date=None,
                    full_history=False, full_refresh=False)
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_resolve_no_args_uses_current_season():
    seasons, date_range = schedules._resolve_seasons_and_dates(_ns())
    assert seasons == [datetime.date.today().year]
    assert date_range is None


def test_resolve_single_season():
    seasons, date_range = schedules._resolve_seasons_and_dates(_ns(season=2024))
    assert seasons == [2024]
    assert date_range is None


def test_resolve_season_range():
    seasons, date_range = schedules._resolve_seasons_and_dates(_ns(start_season=2022, end_season=2024))
    assert seasons == [2022, 2023, 2024]
    assert date_range is None


def test_resolve_single_date():
    seasons, date_range = schedules._resolve_seasons_and_dates(_ns(date='2026-05-08'))
    assert seasons is None
    assert date_range == (datetime.date(2026, 5, 8), datetime.date(2026, 5, 8))


def test_resolve_date_range():
    seasons, date_range = schedules._resolve_seasons_and_dates(
        _ns(start_date='2026-05-01', end_date='2026-05-08'))
    assert seasons is None
    assert date_range == (datetime.date(2026, 5, 1), datetime.date(2026, 5, 8))


def test_resolve_full_history_spans_earliest_to_current_year():
    seasons, date_range = schedules._resolve_seasons_and_dates(_ns(full_history=True))
    assert seasons[0] == schedules.EARLIEST_SEASON
    assert seasons[-1] == datetime.date.today().year
    assert len(seasons) == datetime.date.today().year - schedules.EARLIEST_SEASON + 1
    assert date_range is None


# ---------- _parse_args ----------

def test_parse_args_rejects_mixed_season_and_date(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['schedules', '--season', '2024', '--date', '2024-05-01'])
    with pytest.raises(SystemExit):
        schedules._parse_args()


def test_parse_args_rejects_partial_season_range(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['schedules', '--start-season', '2022'])
    with pytest.raises(SystemExit):
        schedules._parse_args()


def test_parse_args_rejects_partial_date_range(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['schedules', '--end-date', '2026-05-08'])
    with pytest.raises(SystemExit):
        schedules._parse_args()


def test_parse_args_rejects_season_with_season_range(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['schedules', '--season', '2024', '--start-season', '2022', '--end-season', '2024'])
    with pytest.raises(SystemExit):
        schedules._parse_args()


def test_parse_args_rejects_date_with_date_range(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['schedules', '--date', '2026-05-08', '--start-date', '2026-05-01', '--end-date', '2026-05-08'])
    with pytest.raises(SystemExit):
        schedules._parse_args()


def test_parse_args_rejects_full_history_with_season(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['schedules', '--full-history', '--season', '2024'])
    with pytest.raises(SystemExit):
        schedules._parse_args()


def test_parse_args_rejects_full_history_with_date(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['schedules', '--full-history', '--date', '2026-05-08'])
    with pytest.raises(SystemExit):
        schedules._parse_args()


# ---------- _fetch_payloads ----------

def test_fetch_payloads_one_call_per_season(monkeypatch):
    calls = []
    def fake_get(url, params, timeout):
        calls.append(params)
        return _mock_response(_payload([]))
    monkeypatch.setattr(schedules.requests, 'get', fake_get)

    list(schedules._fetch_payloads(seasons=[2023, 2024], date_range=None))

    assert [c['season'] for c in calls] == ['2023', '2024']


def test_fetch_payloads_uses_date_for_single_day(monkeypatch):
    calls = []
    def fake_get(url, params, timeout):
        calls.append(params)
        return _mock_response(_payload([]))
    monkeypatch.setattr(schedules.requests, 'get', fake_get)

    d = datetime.date(2026, 5, 8)
    list(schedules._fetch_payloads(seasons=None, date_range=(d, d)))

    assert calls[0]['date'] == '2026-05-08'
    assert 'startDate' not in calls[0]


def test_fetch_payloads_uses_range_for_date_range(monkeypatch):
    calls = []
    def fake_get(url, params, timeout):
        calls.append(params)
        return _mock_response(_payload([]))
    monkeypatch.setattr(schedules.requests, 'get', fake_get)

    list(schedules._fetch_payloads(
        seasons=None,
        date_range=(datetime.date(2026, 5, 1), datetime.date(2026, 5, 8)),
    ))

    assert calls[0]['startDate'] == '2026-05-01'
    assert calls[0]['endDate'] == '2026-05-08'


# ---------- integration ----------

def test_pipeline_loads_three_tables(tmp_path, fake_make_pipeline, monkeypatch):
    payload = _payload([_hydrated_game(1), _hydrated_game(2)])
    monkeypatch.setattr(schedules.requests, 'get',
                        lambda *a, **kw: _mock_response(payload))

    pipeline = fake_make_pipeline('mlb_statsapi_schedules')
    source = schedules.schedules_source(seasons=[2026])
    pipeline.run(source)

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))

    games = con.execute(
        'SELECT game_pk, away_team_name, home_team_name, attendance '
        'FROM mlb_statsapi_schedules.schedules ORDER BY game_pk'
    ).fetchall()
    assert games == [
        (1, 'Houston Astros', 'Cincinnati Reds', 38432),
        (2, 'Houston Astros', 'Cincinnati Reds', 38432),
    ]

    inning_count = con.execute(
        'SELECT COUNT(*) FROM mlb_statsapi_schedules.schedules_linescore_innings'
    ).fetchone()[0]
    assert inning_count == 4  # 2 games × 2 innings each in fixture

    officials = con.execute(
        'SELECT DISTINCT official_type FROM mlb_statsapi_schedules.schedules_officials ORDER BY official_type'
    ).fetchall()
    assert [r[0] for r in officials] == ['First Base', 'Home Plate', 'Second Base', 'Third Base']


def test_pipeline_merges_on_rerun(tmp_path, fake_make_pipeline, monkeypatch):
    """A second run with an updated game should overwrite the row, not duplicate it."""
    pipeline = fake_make_pipeline('mlb_statsapi_schedules')

    monkeypatch.setattr(schedules.requests, 'get',
                        lambda *a, **kw: _mock_response(_payload([_hydrated_game(1, away_score=3, home_score=2)])))
    pipeline.run(schedules.schedules_source(seasons=[2026]))

    monkeypatch.setattr(schedules.requests, 'get',
                        lambda *a, **kw: _mock_response(_payload([_hydrated_game(1, away_score=10, home_score=0)])))
    pipeline.run(schedules.schedules_source(seasons=[2026]))

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT game_pk, away_score, home_score FROM mlb_statsapi_schedules.schedules'
    ).fetchall()
    assert rows == [(1, 10, 0)]


def test_main_executes(tmp_path, monkeypatch, fake_make_pipeline):
    monkeypatch.setattr(schedules.requests, 'get',
                        lambda *a, **kw: _mock_response(_payload([_hydrated_game(1)])))
    monkeypatch.setattr('loaders.dlt_utils.make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', ['schedules', '--date', '2026-05-08', '--full-refresh'])

    runpy.run_module('loaders.mlb_statsapi.schedules', run_name='__main__')

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    cnt = con.execute('SELECT COUNT(*) FROM mlb_statsapi_schedules.schedules').fetchone()[0]
    assert cnt == 1
