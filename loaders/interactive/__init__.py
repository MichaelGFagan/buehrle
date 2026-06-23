"""Interactive status-and-run tool — bare `buehrle`.

Split per ADR 0001 into a pure, tested :mod:`loaders.interactive.core`
(introspection + plan resolution) and a coverage-omitted
:mod:`loaders.interactive.app` (the Textual TUI: menu, status grid, and the
in-app subprocess runner).
"""
