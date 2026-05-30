import argparse
import datetime
import sys

import pytest

from loaders.cli import (
    add_date_args,
    add_season_args,
    resolve_scope,
    validate_scope_args,
)

EARLIEST_SEASON = 1876


def _make_parser():
    parser = argparse.ArgumentParser()
    add_season_args(parser, EARLIEST_SEASON)
    add_date_args(parser)
    parser.add_argument('--full-refresh', action='store_true')
    return parser


def _parse(argv):
    parser = _make_parser()
    args = parser.parse_args(argv)
    validate_scope_args(parser, args)
    return resolve_scope(args, EARLIEST_SEASON)


# ---------- resolve_scope ----------

def test_resolve_no_args_uses_current_season():
    scope = _parse([])
    assert scope['seasons'] == [datetime.date.today().year]
    assert scope['dates'] is None


def test_resolve_single_season():
    scope = _parse(['--season', '2024'])
    assert scope['seasons'] == [2024]
    assert scope['dates'] is None


def test_resolve_season_range():
    scope = _parse(['--start-season', '2022', '--end-season', '2024'])
    assert scope['seasons'] == [2022, 2023, 2024]
    assert scope['dates'] is None


def test_resolve_single_date():
    scope = _parse(['--date', '2026-05-08'])
    assert scope['seasons'] is None
    assert scope['dates'] == (datetime.date(2026, 5, 8), datetime.date(2026, 5, 8))


def test_resolve_date_range():
    scope = _parse(['--start-date', '2026-05-01', '--end-date', '2026-05-08'])
    assert scope['seasons'] is None
    assert scope['dates'] == (datetime.date(2026, 5, 1), datetime.date(2026, 5, 8))


def test_resolve_full_history_spans_earliest_to_current_year():
    scope = _parse(['--full-history'])
    seasons = scope['seasons']
    assert seasons[0] == EARLIEST_SEASON
    assert seasons[-1] == datetime.date.today().year
    assert len(seasons) == datetime.date.today().year - EARLIEST_SEASON + 1
    assert scope['dates'] is None


# ---------- validate_scope_args ----------

@pytest.mark.parametrize('argv', [
    ['--season', '2024', '--date', '2024-05-01'],
    ['--start-season', '2022'],
    ['--end-date', '2026-05-08'],
    ['--season', '2024', '--start-season', '2022', '--end-season', '2024'],
    ['--date', '2026-05-08', '--start-date', '2026-05-01', '--end-date', '2026-05-08'],
    ['--full-history', '--season', '2024'],
    ['--full-history', '--date', '2026-05-08'],
])
def test_validate_rejects_invalid_scope_combinations(argv):
    parser = _make_parser()
    args = parser.parse_args(argv)
    with pytest.raises(SystemExit):
        validate_scope_args(parser, args)
