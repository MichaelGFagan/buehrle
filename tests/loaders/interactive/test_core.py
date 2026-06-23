"""Tests for the interactive grid's pure logic (loaders/interactive/core.py).

The Textual TUI + subprocess runner in app.py are coverage-omitted; all
plan resolution and loader introspection is exercised here.
"""

import argparse
import datetime
from types import SimpleNamespace

import pytest

from loaders.interactive import core


TODAY = datetime.date(2026, 6, 20)


# --- loader introspection --------------------------------------------------

def _season_only_module():
    """A stand-in loader registering a season-scoped subcommand."""
    def register(subparsers):
        parser = subparsers.add_parser('fake-seasonal')
        parser.add_argument('--start-season', type=int)
        parser.add_argument('--full-history', action='store_true')
        parser.add_argument('--full-refresh', action='store_true')

    return SimpleNamespace(PIPELINE_NAME='fake_seasonal', WATERMARKS={'t': 'season'},
                           register=register)


def _date_capable_module():
    def register(subparsers):
        parser = subparsers.add_parser('fake-dated')
        parser.add_argument('--start-season', type=int)
        parser.add_argument('--start-date')
        parser.add_argument('--end-date')
        parser.add_argument('--full-history', action='store_true')
        parser.add_argument('--full-refresh', action='store_true')

    return SimpleNamespace(PIPELINE_NAME='fake_dated', WATERMARKS={'t': 'game_date'},
                           register=register)


def test_command_name_recovers_cli_token():
    assert core.command_name(_season_only_module()) == 'fake-seasonal'
    assert core.command_name(_date_capable_module()) == 'fake-dated'


def test_accepts_dates():
    assert core.accepts_dates(_season_only_module()) is False
    assert core.accepts_dates(_date_capable_module()) is True


def test_command_name_on_real_loader():
    from loaders.mlb_statsapi import schedules
    assert core.command_name(schedules) == 'mlb-statsapi-schedules'
    assert core.accepts_dates(schedules) is True


# --- watermark parsing -----------------------------------------------------

@pytest.mark.parametrize('value, year', [
    ('2024', 2024),
    ('2024-04-28', 2024),
    ('20240428', 2024),
    (None, None),
    ('Date', None),         # leaked CSV header -> unparseable
    ('24', None),
])
def test_watermark_year(value, year):
    assert core.watermark_year(value) == year


@pytest.mark.parametrize('value, iso', [
    ('2024-04-28', '2024-04-28'),
    ('20240428', '2024-04-28'),
    ('2024', None),         # a bare season is not a date
    (None, None),
    ('Date', None),
])
def test_watermark_iso_date(value, iso):
    assert core.watermark_iso_date(value) == iso


# --- plan resolution -------------------------------------------------------

def test_full_action_watermarked_loader():
    flags = core.plan_flags(action=core.FULL, full_refresh_only=False,
                            oldest='2020', loader_accepts_dates=False, today=TODAY)
    assert flags == ['--full-refresh', '--full-history']


def test_full_action_single_shot_loader_omits_full_history():
    flags = core.plan_flags(action=core.FULL, full_refresh_only=True,
                            oldest=None, loader_accepts_dates=False, today=TODAY)
    assert flags == ['--full-refresh']


def test_incremental_from_season_watermark():
    flags = core.plan_flags(action=core.INCREMENTAL, full_refresh_only=False,
                            oldest='2022', loader_accepts_dates=False, today=TODAY)
    assert flags == ['--start-season', '2022', '--end-season', '2026']


def test_incremental_date_loader_uses_date_scope():
    flags = core.plan_flags(action=core.INCREMENTAL, full_refresh_only=False,
                            oldest='2026-04-28', loader_accepts_dates=True, today=TODAY)
    assert flags == ['--start-date', '2026-04-28', '--end-date', '2026-06-20']


def test_incremental_date_watermark_but_season_only_loader_falls_back_to_year():
    # retrosheet game_logs/schedules: date watermark, but the CLI takes seasons.
    flags = core.plan_flags(action=core.INCREMENTAL, full_refresh_only=False,
                            oldest='2024-04-28', loader_accepts_dates=False, today=TODAY)
    assert flags == ['--start-season', '2024', '--end-season', '2026']


def test_incremental_never_loaded_falls_back_to_full_history():
    flags = core.plan_flags(action=core.INCREMENTAL, full_refresh_only=False,
                            oldest=None, loader_accepts_dates=True, today=TODAY)
    assert flags == ['--full-history']


def test_incremental_unparseable_watermark_falls_back_to_full_history():
    flags = core.plan_flags(action=core.INCREMENTAL, full_refresh_only=False,
                            oldest='Date', loader_accepts_dates=False, today=TODAY)
    assert flags == ['--full-history']


def test_incremental_invalid_for_full_refresh_only():
    with pytest.raises(ValueError):
        core.plan_flags(action=core.INCREMENTAL, full_refresh_only=True,
                        oldest=None, loader_accepts_dates=False, today=TODAY)


def test_plan_flags_rejects_unknown_action():
    with pytest.raises(ValueError):
        core.plan_flags(action='bogus', full_refresh_only=False,
                        oldest='2020', loader_accepts_dates=False, today=TODAY)


# --- LoaderRow + build_row -------------------------------------------------

def _status(**kw):
    base = dict(schema='s', table_count=1, last_load=None, load_count=1,
                watermarks={'t': '2022'}, oldest='2022', full_refresh_only=False)
    base.update(kw)
    return SimpleNamespace(**base)


def test_build_row_pulls_cli_identity():
    row = core.build_row(_season_only_module(), _status())
    assert row.command == 'fake-seasonal'
    assert row.accepts_dates is False
    assert row.oldest == '2022'


def test_cycle_watermarked():
    row = core.build_row(_season_only_module(), _status())
    assert row.selection is None
    row.cycle(); assert row.selection == core.INCREMENTAL
    row.cycle(); assert row.selection == core.FULL
    row.cycle(); assert row.selection is None


def test_cycle_full_refresh_only_skips_incremental():
    row = core.build_row(_season_only_module(), _status(full_refresh_only=True))
    assert row.can_incremental() is False
    row.cycle(); assert row.selection == core.FULL
    row.cycle(); assert row.selection is None


def test_toggle_incremental_noop_for_full_refresh_only():
    row = core.build_row(_season_only_module(), _status(full_refresh_only=True))
    row.toggle_incremental()
    assert row.selection is None


def test_toggle_incremental():
    row = core.build_row(_season_only_module(), _status())
    row.toggle_incremental(); assert row.selection == core.INCREMENTAL
    row.toggle_incremental(); assert row.selection is None


def test_toggle_full():
    row = core.build_row(_season_only_module(), _status())
    row.toggle_full(); assert row.selection == core.FULL
    row.toggle_full(); assert row.selection is None


def test_row_flags_none_when_unselected():
    row = core.build_row(_season_only_module(), _status())
    assert row.flags(TODAY) is None


# --- top-level menu --------------------------------------------------------

def test_menu_items_cover_grid_and_utilities():
    items = core.menu_items()
    kinds = [it.kind for it in items]
    assert core.GRID in kinds and core.QUIT in kinds
    commands = {it.command for it in items if it.kind == core.RUN}
    # mirrors the top-level CLI utilities (loaders.registry.utilities), minus
    # `state` whose info is available in the grid (Load data).
    assert commands == {'retrosheet-sync', 'install-chadwick', 'drop-db'}


def test_menu_item_keys_are_unique():
    keys = [it.key for it in core.menu_items()]
    assert len(keys) == len(set(keys))


def test_menu_command_names_match_registered_utilities():
    from loaders.registry import utilities
    registered = {core.command_name(m) for m in utilities()}
    menu_commands = {it.command for it in core.menu_items() if it.kind == core.RUN}
    # `state` is a CLI utility but intentionally absent from the menu (its info
    # is shown in the grid).
    assert menu_commands == registered - {'state'}


def test_drop_db_item_is_confirmed_and_passes_yes():
    drop = next(it for it in core.menu_items() if it.command == 'drop-db')
    assert drop.confirm is not None
    assert '--yes' in drop.flags


def test_menu_argv_forwards_db_when_wanted():
    drop = next(it for it in core.menu_items() if it.command == 'drop-db')
    assert core.menu_argv(drop, db='/tmp/x.duckdb')[-2:] == ['--db', '/tmp/x.duckdb']


def test_menu_argv_omits_db_when_not_wanted():
    sync = next(it for it in core.menu_items() if it.command == 'retrosheet-sync')
    assert core.menu_argv(sync, db='/tmp/x.duckdb') == ['retrosheet-sync']


def test_menu_argv_includes_static_flags_and_db():
    drop = next(it for it in core.menu_items() if it.command == 'drop-db')
    assert core.menu_argv(drop, db='/tmp/x.duckdb') == ['drop-db', '--yes', '--db', '/tmp/x.duckdb']


def test_menu_argv_rejects_non_run_item():
    grid = next(it for it in core.menu_items() if it.kind == core.GRID)
    with pytest.raises(ValueError):
        core.menu_argv(grid)


def test_selected_commands_collects_in_order():
    season = core.build_row(_season_only_module(), _status(oldest='2021'))
    dated = core.build_row(_date_capable_module(), _status(oldest='2026-04-28'))
    season.selection = core.INCREMENTAL
    dated.selection = core.FULL
    plans = core.selected_commands([season, dated], TODAY)
    assert plans == [
        ('fake-seasonal', ['--start-season', '2021', '--end-season', '2026']),
        ('fake-dated', ['--full-refresh', '--full-history']),
    ]


# --- jobs (TUI subprocess units) -------------------------------------------

def test_loader_argv_prefixes_load():
    assert core.loader_argv('fangraphs', ['--season', '2024']) == \
        ['load', 'fangraphs', '--season', '2024']


def test_loader_jobs_build_from_selection():
    season = core.build_row(_season_only_module(), _status(oldest='2021'))
    season.selection = core.INCREMENTAL
    unselected = core.build_row(_date_capable_module(), _status())
    jobs = core.loader_jobs([season, unselected], TODAY)
    assert len(jobs) == 1
    assert jobs[0].label == 'fake-seasonal'
    assert jobs[0].argv_tail == ['load', 'fake-seasonal', '--start-season', '2021', '--end-season', '2026']


def test_utility_job_forwards_db_and_flags():
    drop = next(it for it in core.menu_items() if it.command == 'drop-db')
    job = core.utility_job(drop, db='/tmp/x.duckdb')
    assert job.label == 'drop-db'
    assert job.argv_tail == ['drop-db', '--yes', '--db', '/tmp/x.duckdb']
