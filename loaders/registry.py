"""Canonical registry of buehrle loaders.

Lives apart from ``__main__`` so the CLI entry point, the ``state`` status
view, and the interactive grid can all share one loader list without an
import cycle (``state`` imports this lazily; this imports ``state``).
"""

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


def data_loaders():
    """Loaders that land data into a schema — those declaring ``WATERMARKS``.

    Excludes utilities (``state``, ``retrosheet_sync``, ``install_chadwick``)
    that register a subcommand but don't own a destination schema.
    """
    return [module for module in LOADERS if hasattr(module, 'WATERMARKS')]
