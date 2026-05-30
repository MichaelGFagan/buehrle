"""Unified entry point for buehrle loaders.

Usage:
    buehrle <loader> [args...]
    python -m loaders <loader> [args...]

Each loader module exposes a `register(subparsers)` function that adds its
subcommand and a `main(parser, args)` function that runs it. See
loaders/fangraphs/fangraphs.py for the reference implementation.
"""

import argparse

from loaders.baseball_reference import baseball_reference_draft_results, baseball_reference_war
from loaders.chadwick import chadwick_register
from loaders.fangraphs import fangraphs
from loaders.lahman import lahman
from loaders.mlb_statsapi import schedules as mlb_statsapi_schedules
from loaders.retrosheet import (
    install_chadwick,
    retrosheet_events,
    retrosheet_game_logs,
    retrosheet_rosters,
    retrosheet_schedules,
    retrosheet_sync,
    retrosheet_umpires,
)
from loaders.statcast import (
    statcast_batting_leaderboards,
    statcast_fielding_leaderboards,
    statcast_pitches,
    statcast_pitching_leaderboards,
    statcast_running_leaderboards,
)
from loaders import state


LOADERS = [
    baseball_reference_draft_results,
    baseball_reference_war,
    chadwick_register,
    fangraphs,
    install_chadwick,
    lahman,
    mlb_statsapi_schedules,
    retrosheet_events,
    retrosheet_game_logs,
    retrosheet_rosters,
    retrosheet_schedules,
    retrosheet_sync,
    retrosheet_umpires,
    state,
    statcast_batting_leaderboards,
    statcast_fielding_leaderboards,
    statcast_pitches,
    statcast_pitching_leaderboards,
    statcast_running_leaderboards,
]


def main():
    parser = argparse.ArgumentParser(prog='buehrle')
    subparsers = parser.add_subparsers(dest='loader', required=True, metavar='<loader>')
    for module in LOADERS:
        module.register(subparsers)
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
