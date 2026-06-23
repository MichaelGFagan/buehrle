"""Textual TUI for bare ``buehrle``: a persistent full-screen app fronting the
loader status grid and the CLI utilities.

Coverage-omitted (see pyproject ``[tool.coverage.run] omit``): this is the
untestable UI/IO half. All resolution logic lives in
:mod:`loaders.interactive.core`, which is tested.

Everything stays inside the alternate screen: selecting a loader run or a
utility streams its subprocess output into an in-app log pane rather than
dropping back to the terminal.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import duckdb
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Footer, Header, Label, Log, OptionList
from textual.widgets.option_list import Option

from loaders.interactive.core import (
    FULL,
    GRID,
    INCREMENTAL,
    QUIT,
    RUN,
    Job,
    LoaderRow,
    MenuItem,
    build_row,
    loader_jobs,
    menu_items,
    selected_commands,
    utility_job,
)
from loaders.interactive.runner import stream_jobs
from loaders.state import DEFAULT_DB, LoaderStatus, fmt, loader_status


def _never_loaded_status(module) -> LoaderStatus:
    return LoaderStatus(
        schema=module.PIPELINE_NAME,
        table_count=0,
        last_load=None,
        load_count=0,
        watermarks={table: None for table in module.WATERMARKS},
        oldest=None,
        full_refresh_only=not module.WATERMARKS,
    )


def load_rows(db: Path) -> list[LoaderRow]:
    """Read every registered loader's status into grid rows.

    A missing DB file means nothing has been loaded yet, so every row is
    synthesised as never-loaded rather than erroring.
    """
    from loaders.registry import data_loaders

    modules = data_loaders()
    if db.exists():
        con = duckdb.connect(str(db), read_only=True)
        try:
            statuses = [loader_status(con, module) for module in modules]
        finally:
            con.close()
    else:
        statuses = [_never_loaded_status(module) for module in modules]
    return [build_row(module, status) for module, status in zip(modules, statuses)]


# --- run screen ------------------------------------------------------------

class RunScreen(Screen):
    """Streams one or more jobs' subprocess output into an in-app log.

    Jobs run sequentially (continue-on-error). The screen stays up after they
    finish so the output remains readable; Escape returns to the previous
    screen.
    """

    BINDINGS = [Binding('escape', 'back', 'Back', show=True)]

    def __init__(self, jobs: list[Job], intro: list[str] | None = None) -> None:
        super().__init__()
        self._jobs = jobs
        self._intro = intro or []
        # NB: do not name this `_running` — that's MessagePump's internal loop
        # flag; clobbering it stops the screen from ever processing messages.
        self._jobs_running = True

    def compose(self) -> ComposeResult:
        yield Header()
        yield Log(highlight=False, max_lines=10_000, id='output')
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = 'running…'
        # Query the Log here, on the UI thread where it is mounted; the worker
        # thread only writes to it via call_from_thread.
        log = self.query_one('#output', Log)
        for line in self._intro:
            log.write_line(line)
        self._run_jobs(log)

    def action_back(self) -> None:
        # Don't abandon a run mid-flight.
        if not self._jobs_running:
            self.dismiss()

    @work(thread=True)
    def _run_jobs(self, log: Log) -> None:
        # Runs off the event loop so a blocking subprocess never freezes the UI.
        def write(line: str) -> None:
            self.app.call_from_thread(log.write_line, line)

        stream_jobs(self._jobs, write)
        self.app.call_from_thread(self._on_finished)

    def _on_finished(self) -> None:
        self._jobs_running = False
        self.sub_title = 'done — press Esc to return'


# --- confirm modal ---------------------------------------------------------

class ConfirmScreen(ModalScreen[bool]):
    """A y/n confirmation for destructive actions.

    Yes/No are selectable: Left/Right move the highlight (clamped, no wrap),
    Enter chooses the highlighted option, and y/n remain direct hotkeys. The
    highlight defaults to No, the safe choice for a destructive action.
    """

    BINDINGS = [
        Binding('left', 'move(-1)', 'Left'),
        Binding('right', 'move(1)', 'Right'),
        Binding('enter', 'select', 'Select'),
        Binding('y', 'confirm', 'Yes'),
        Binding('n,escape', 'cancel', 'No'),
    ]

    def __init__(self, prompt: str) -> None:
        super().__init__()
        self._prompt = prompt
        self._index = 1   # 0 = Yes, 1 = No; default to the safe choice

    def compose(self) -> ComposeResult:
        with Vertical(id='confirm-box'):
            yield Label(self._prompt)
            with Horizontal(id='confirm-options'):
                yield Label('[y]es', id='confirm-yes', markup=False)
                yield Label('[n]o', id='confirm-no', markup=False)

    def on_mount(self) -> None:
        self._refresh_options()

    def _refresh_options(self) -> None:
        self.query_one('#confirm-yes', Label).set_class(self._index == 0, '-selected')
        self.query_one('#confirm-no', Label).set_class(self._index == 1, '-selected')

    def action_move(self, delta: int) -> None:
        self._index = max(0, min(1, self._index + delta))
        self._refresh_options()

    def action_select(self) -> None:
        self.dismiss(self._index == 0)

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


# --- grid screen -----------------------------------------------------------

_GRID_COLUMNS = ('inc', 'full', 'loader', 'last load', 'watermark')


class GridScreen(Screen):
    """The loader status grid: per-row incremental / full-refresh toggles."""

    BINDINGS = [
        Binding('i', 'toggle_incremental', 'Incremental'),
        Binding('f', 'toggle_full', 'Full refresh'),
        Binding('space', 'cycle', 'Cycle'),
        Binding('enter', 'run', 'Run selected'),
        Binding('escape', 'back', 'Back'),
    ]

    def __init__(self, rows: list[LoaderRow], today: datetime.date) -> None:
        super().__init__()
        self._rows = rows
        self._today = today

    def compose(self) -> ComposeResult:
        yield Header()
        yield DataTable(id='grid', cursor_type='row', zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = 'select loads — Enter to run'
        table = self.query_one('#grid', DataTable)
        table.add_columns(*_GRID_COLUMNS)
        for row in self._rows:
            table.add_row(*self._cells(row))

    def _cells(self, row: LoaderRow) -> tuple[str, ...]:
        inc = 'n/a' if not row.can_incremental() else ('[X]' if row.selection == INCREMENTAL else '[ ]')
        full = '[X]' if row.selection == FULL else '[ ]'
        return (inc, full, row.command, fmt(row.last_load), fmt(row.oldest))

    def _refresh_cursor_row(self) -> None:
        table = self.query_one('#grid', DataTable)
        index = table.cursor_row
        row = self._rows[index]
        for column, value in enumerate(self._cells(row)):
            table.update_cell_at((index, column), value)

    def action_toggle_incremental(self) -> None:
        self._rows[self.query_one('#grid', DataTable).cursor_row].toggle_incremental()
        self._refresh_cursor_row()

    def action_toggle_full(self) -> None:
        self._rows[self.query_one('#grid', DataTable).cursor_row].toggle_full()
        self._refresh_cursor_row()

    def action_cycle(self) -> None:
        self._rows[self.query_one('#grid', DataTable).cursor_row].cycle()
        self._refresh_cursor_row()

    def action_back(self) -> None:
        self.dismiss()

    def action_run(self) -> None:
        jobs = loader_jobs(self._rows, self._today)
        if not jobs:
            self.notify('Nothing selected.', severity='warning')
            return
        plans = selected_commands(self._rows, self._today)
        intro = ['Planned loads:']
        intro += [f'  buehrle load {command} {" ".join(flags)}' for command, flags in plans]
        if any('--full-refresh' in flags for _, flags in plans):
            intro.append('Note: full-refresh rebuilds drop and reload from scratch and can be slow.')
        self.app.push_screen(RunScreen(jobs, intro=intro))


# --- main menu -------------------------------------------------------------

class MenuScreen(Screen):
    """Top-level menu: the loader grid plus every CLI utility."""

    BINDINGS = [Binding('q', 'quit', 'Quit')]

    def __init__(self, db: Path) -> None:
        super().__init__()
        self._db = db
        self._items = menu_items()

    def compose(self) -> ComposeResult:
        yield Header()
        yield OptionList(
            *[Option(f'({item.key})  {item.label}', id=str(i))
              for i, item in enumerate(self._items)],
            id='menu',
        )
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = 'choose an action'

    def on_key(self, event) -> None:
        # Single-key hotkeys mirroring each item's `key`.
        for item in self._items:
            if event.key == item.key and item.kind != QUIT:
                event.stop()
                self._dispatch(item)
                return

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        self._dispatch(self._items[int(event.option.id)])

    def action_quit(self) -> None:
        self.app.exit(0)

    def _dispatch(self, item: MenuItem) -> None:
        if item.kind == QUIT:
            self.app.exit(0)
        elif item.kind == GRID:
            today = datetime.date.today()
            self.app.push_screen(GridScreen(load_rows(self._db), today))
        elif item.kind == RUN:
            if item.confirm:
                self.app.push_screen(ConfirmScreen(item.confirm), self._run_after_confirm(item))
            else:
                self._run_utility(item)

    def _run_after_confirm(self, item: MenuItem):
        def callback(confirmed: bool) -> None:
            if confirmed:
                self._run_utility(item)
        return callback

    def _run_utility(self, item: MenuItem) -> None:
        job = utility_job(item, db=str(self._db))
        self.app.push_screen(RunScreen([job]))


# --- app -------------------------------------------------------------------

class BuehrleApp(App):
    CSS = """
    #output { border: round $accent; }
    #confirm-box {
        width: 60; height: auto; padding: 1 2;
        border: round $warning; background: $surface;
        align: center middle;
    }
    #confirm-options { width: auto; height: auto; margin-top: 1; align: center middle; }
    #confirm-options Label { padding: 0 2; }
    #confirm-options Label.-selected { background: $accent; color: $text; text-style: bold; }
    ConfirmScreen { align: center middle; }
    """
    TITLE = 'buehrle'

    def __init__(self, db: Path) -> None:
        super().__init__()
        self._db = db

    def on_mount(self) -> None:
        self.push_screen(MenuScreen(self._db))


def run(db: Path | None = None) -> int:
    """Entry point for bare ``buehrle``. Returns a process exit code."""
    db = Path(db) if db is not None else DEFAULT_DB
    result = BuehrleApp(db).run()
    return result if isinstance(result, int) else 0
