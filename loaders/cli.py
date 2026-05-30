"""Shared CLI helpers for loaders following the standard scope convention.

See CLAUDE.md > "Loader CLI conventions" for the spec, and
loaders/mlb_statsapi/schedules.py for the reference implementation.
"""

import argparse
import datetime

from typing import Callable


def add_season_args(parser: argparse.ArgumentParser, earliest_season: int) -> None:
    parser.add_argument('--season', type=int)
    parser.add_argument('--start-season', type=int)
    parser.add_argument('--end-season', type=int)
    parser.add_argument('--full-history', action='store_true',
                        help=f'Backfill from {earliest_season} through the current year.')


def validate_season_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    season_args = any([args.season, args.start_season, args.end_season])
    if args.full_history and season_args:
        parser.error('--full-history is mutually exclusive with --season / --start-season / --end-season')
    if (args.start_season is None) != (args.end_season is None):
        parser.error('--start-season and --end-season must be provided together')
    if args.season and (args.start_season or args.end_season):
        parser.error('--season is mutually exclusive with --start-season / --end-season')


def resolve_seasons(args: argparse.Namespace, earliest_season: int) -> tuple[int, int]:
    today_year = datetime.date.today().year
    if args.full_history:
        return earliest_season, today_year
    if args.season:
        return args.season, args.season
    if args.start_season:
        return args.start_season, args.end_season
    return today_year, today_year


def add_date_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--date')
    parser.add_argument('--start-date')
    parser.add_argument('--end-date')


def validate_scope_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Combined season + date validation. Use instead of validate_season_args
    when the loader also has date args."""
    validate_season_args(parser, args)

    season_args = any([args.season, args.start_season, args.end_season])
    date_args = any([args.date, args.start_date, args.end_date])
    if season_args and date_args:
        parser.error('Cannot mix season args (--season, --start-season, --end-season) '
                     'with date args (--date, --start-date, --end-date)')
    if args.full_history and date_args:
        parser.error('--full-history is mutually exclusive with date args')
    if (args.start_date is None) != (args.end_date is None):
        parser.error('--start-date and --end-date must be provided together')
    if args.date and (args.start_date or args.end_date):
        parser.error('--date is mutually exclusive with --start-date / --end-date')


def resolve_scope(args: argparse.Namespace, earliest_season: int) -> dict:
    """Returns {'seasons': list[int] | None, 'dates': (start, end) | None}.
    Exactly one of the two keys is populated."""
    if args.date:
        d = datetime.date.fromisoformat(args.date)
        return {'seasons': None, 'dates': (d, d)}
    if args.start_date:
        return {'seasons': None,
                'dates': (datetime.date.fromisoformat(args.start_date),
                          datetime.date.fromisoformat(args.end_date))}
    start, end = resolve_seasons(args, earliest_season)
    return {'seasons': list(range(start, end + 1)), 'dates': None}


def resolve_dates(
    args: argparse.Namespace,
    earliest_season: int,
    season_bounds: Callable[[int], tuple[datetime.date, datetime.date]],
) -> tuple[datetime.date, datetime.date]:
    """Always returns a (start, end) date range. Expands seasons via season_bounds(year)."""
    scope = resolve_scope(args, earliest_season)
    if scope['dates'] is not None:
        return scope['dates']
    seasons = scope['seasons']
    return season_bounds(seasons[0])[0], season_bounds(seasons[-1])[1]


def add_resources_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--resources', nargs='+', default=None,
                        help='Subset of resource names to load. Defaults to all.')


def apply_resources(source, args: argparse.Namespace):
    """Filter a dlt source to the resources named in --resources. Raises if any name is unknown."""
    if not args.resources:
        return source
    available = set(source.resources.keys())
    unknown = sorted(set(args.resources) - available)
    if unknown:
        raise SystemExit(
            f'Unknown resources: {unknown}. Available: {sorted(available)}'
        )
    return source.with_resources(*args.resources)
