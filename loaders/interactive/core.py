"""Pure logic for the interactive status grid: loader introspection and the
watermark-driven plan resolver.

Kept free of any UI or subprocess code so it can be unit-tested under the
``fail_under = 90`` gate; the Textual TUI and the runner live in
:mod:`loaders.interactive.app`, which is omitted from coverage.

Two grid actions, both translated here into the flags a loader's CLI subcommand
already understands (see CLAUDE.md > "Loader CLI conventions"):

- **incremental** — smart, watermark-driven. Re-load from the loader's *oldest*
  table watermark (inclusive — cheap insurance, dlt merges idempotently) through
  today. A loader that isn't fully current (``oldest is None``) falls back to a
  full backfill (``--full-history``) with no refresh.
- **full** — a clean rebuild: ``--full-refresh --full-history`` (or just
  ``--full-refresh`` for single-shot, full-refresh-only loaders).
"""

from __future__ import annotations

import argparse
import datetime
import re
from dataclasses import dataclass, field

INCREMENTAL = 'incremental'
FULL = 'full'

# Top-level menu item kinds.
GRID = 'grid'   # open the loader status grid (in-app)
RUN = 'run'     # run a utility subcommand as a subprocess
QUIT = 'quit'   # exit

_SEASON_RE = re.compile(r'^\d{4}$')
_ISO_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_COMPACT_DATE_RE = re.compile(r'^\d{8}$')


def _build_subparser(module) -> tuple[str, argparse.ArgumentParser]:
    """Re-run a loader's ``register`` against a throwaway parser to recover the
    subcommand name (and parser) it declares. The name is the CLI token
    (e.g. ``mlb-statsapi-schedules``), which differs from ``PIPELINE_NAME``
    (e.g. ``mlb_statsapi_schedules``) and isn't stored on the module.
    """
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    module.register(sub)
    name = next(iter(sub.choices))
    return name, sub.choices[name]


def command_name(module) -> str:
    """The ``buehrle load <name>`` token a loader registers."""
    return _build_subparser(module)[0]


def accepts_dates(module) -> bool:
    """Whether a loader's CLI accepts ``--start-date`` / ``--end-date``."""
    _, parser = _build_subparser(module)
    return '--start-date' in parser._option_string_actions


def watermark_year(value: str | None) -> int | None:
    """Extract a 4-digit year from a watermark string, or ``None`` if it doesn't
    parse. Handles bare seasons (``'2024'``), ISO dates (``'2024-04-28'``) and
    compact dates (``'20240428'``); anything else (e.g. a leaked CSV header) is
    unparseable and signals a fall back to full-history.
    """
    if value is None:
        return None
    if _SEASON_RE.match(value) or _ISO_DATE_RE.match(value) or _COMPACT_DATE_RE.match(value):
        return int(value[:4])
    return None


def watermark_iso_date(value: str | None) -> str | None:
    """Normalise a date watermark to ISO ``YYYY-MM-DD``, or ``None`` if it isn't
    a date (a bare season is not a date)."""
    if value is None:
        return None
    if _ISO_DATE_RE.match(value):
        return value
    if _COMPACT_DATE_RE.match(value):
        return f'{value[:4]}-{value[4:6]}-{value[6:8]}'
    return None


def plan_flags(
    *,
    action: str,
    full_refresh_only: bool,
    oldest: str | None,
    loader_accepts_dates: bool,
    today: datetime.date,
) -> list[str]:
    """The CLI flags to append after ``buehrle load <name>`` for one selection.

    ``today`` is injected (not read from the clock) so the resolver stays pure
    and testable.
    """
    if action == FULL:
        # Single-shot loaders take only --full-refresh; they have no scope to
        # backfill over.
        return ['--full-refresh'] if full_refresh_only else ['--full-refresh', '--full-history']

    if action != INCREMENTAL:
        raise ValueError(f'unknown action: {action!r}')
    if full_refresh_only:
        raise ValueError('incremental is not a valid action for a full-refresh-only loader')

    # Not fully current (never loaded, or a laggard table is absent/empty):
    # backfill everything, merging — no refresh.
    if oldest is None:
        return ['--full-history']

    # Prefer date scope only when the loader accepts dates *and* the watermark
    # is itself a date; otherwise fall back to the loader's season scope using
    # the watermark's year.
    iso = watermark_iso_date(oldest)
    if loader_accepts_dates and iso is not None:
        return ['--start-date', iso, '--end-date', today.isoformat()]

    year = watermark_year(oldest)
    if year is None:
        return ['--full-history']
    return ['--start-season', str(year), '--end-season', str(today.year)]


@dataclass
class LoaderRow:
    """One grid row: a loader's module, its CLI identity, and current status.

    ``selection`` is the user's choice — ``None``, ``INCREMENTAL`` or ``FULL``.
    """
    module: object
    command: str
    schema: str
    accepts_dates: bool
    full_refresh_only: bool
    table_count: int
    last_load: object          # datetime | None
    load_count: int
    oldest: str | None
    selection: str | None = None

    def can_incremental(self) -> bool:
        """Full-refresh-only loaders offer no incremental mode."""
        return not self.full_refresh_only

    def cycle(self) -> None:
        """Advance the selection: none -> incremental -> full -> none.

        Skips incremental for full-refresh-only loaders (none -> full -> none).
        """
        if self.full_refresh_only:
            self.selection = FULL if self.selection is None else None
            return
        self.selection = {
            None: INCREMENTAL,
            INCREMENTAL: FULL,
            FULL: None,
        }[self.selection]

    def toggle_incremental(self) -> None:
        if not self.can_incremental():
            return
        self.selection = None if self.selection == INCREMENTAL else INCREMENTAL

    def toggle_full(self) -> None:
        self.selection = None if self.selection == FULL else FULL

    def flags(self, today: datetime.date) -> list[str] | None:
        """Resolved CLI flags for this row's selection, or ``None`` if unset."""
        if self.selection is None:
            return None
        return plan_flags(
            action=self.selection,
            full_refresh_only=self.full_refresh_only,
            oldest=self.oldest,
            loader_accepts_dates=self.accepts_dates,
            today=today,
        )


def build_row(module, status) -> LoaderRow:
    """Combine a loader module with its :class:`loaders.state.LoaderStatus`."""
    return LoaderRow(
        module=module,
        command=command_name(module),
        schema=status.schema,
        accepts_dates=accepts_dates(module),
        full_refresh_only=status.full_refresh_only,
        table_count=status.table_count,
        last_load=status.last_load,
        load_count=status.load_count,
        oldest=status.oldest,
    )


@dataclass(frozen=True)
class MenuItem:
    """One entry on the top-level interactive menu.

    ``kind`` is :data:`GRID`, :data:`RUN`, or :data:`QUIT`. For ``RUN`` items,
    ``command`` + ``flags`` are the ``buehrle`` subcommand to invoke (these
    mirror the top-level CLI utilities so the menu stays a superset of the
    command line). ``confirm``, when set, is shown as a y/N prompt before
    running — used for destructive actions.
    """
    key: str
    label: str
    kind: str
    command: str | None = None
    flags: tuple[str, ...] = field(default_factory=tuple)
    confirm: str | None = None
    wants_db: bool = False   # forward the active --db path to the subcommand


def menu_items() -> list[MenuItem]:
    """The top-level menu: the loader grid plus every CLI utility.

    The command names match those registered by the utility modules
    (``state``, ``retrosheet_sync``, ``install_chadwick``, ``drop_db``); see
    :func:`loaders.registry.utilities`.
    """
    return [
        MenuItem('l', 'Load data (status grid)', GRID),
        MenuItem('r', 'Clone / update the Retrosheet repo', RUN, command='retrosheet-sync'),
        MenuItem('c', 'Install Chadwick tools (Homebrew)', RUN, command='install-chadwick'),
        MenuItem('d', 'Drop the raw database', RUN, command='drop-db',
                 flags=('--yes',), confirm='Delete the raw DuckDB database? This cannot be undone.',
                 wants_db=True),
        MenuItem('q', 'Quit', QUIT),
    ]


def menu_argv(item: MenuItem, db: str | None = None) -> list[str]:
    """The argv tail (after ``python -m loaders``) for a ``RUN`` menu item.

    Appends ``--db <db>`` when the item opts in via ``wants_db`` and a path is
    given, so utilities operate on the same database as the grid.
    """
    if item.kind != RUN or item.command is None:
        raise ValueError(f'{item.label!r} is not a runnable menu item')
    argv = [item.command, *item.flags]
    if item.wants_db and db is not None:
        argv += ['--db', str(db)]
    return argv


def selected_commands(rows: list[LoaderRow], today: datetime.date) -> list[tuple[str, list[str]]]:
    """``(command, flags)`` pairs for every selected row, in grid order."""
    plans = []
    for row in rows:
        flags = row.flags(today)
        if flags is not None:
            plans.append((row.command, flags))
    return plans


def loader_argv(command: str, flags: list[str]) -> list[str]:
    """The argv tail (after ``python -m loaders``) to run one loader."""
    return ['load', command, *flags]


@dataclass
class Job:
    """A unit of work the TUI runs as a subprocess: a display label and the
    argv tail passed to ``python -m loaders``."""
    label: str
    argv_tail: list[str]


def loader_jobs(rows: list[LoaderRow], today: datetime.date) -> list[Job]:
    """Jobs for every selected grid row, in order."""
    return [
        Job(label=command, argv_tail=loader_argv(command, flags))
        for command, flags in selected_commands(rows, today)
    ]


def utility_job(item: MenuItem, db: str | None = None) -> Job:
    """The job for a runnable (``RUN``) menu item."""
    return Job(label=item.command, argv_tail=menu_argv(item, db=db))
