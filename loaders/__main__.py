"""Unified entry point for buehrle loaders.

Usage:
    buehrle                           # interactive status grid (no args)
    buehrle load <loader> [args...]   # run a data loader
    buehrle <utility> [args...]       # run a utility (state, drop-db, ...)
    python -m loaders load <loader> [args...]

Bare `buehrle` launches the interactive status-and-run grid (see
loaders/interactive/). Data loaders live under the `load` subcommand;
utilities (which don't land data into a schema) sit at the top level. Each
module exposes a `register(subparsers)` function that adds its subcommand and a
`main(parser, args)` function that runs it. See loaders/fangraphs/fangraphs.py
for the reference loader implementation.
"""

import argparse
import functools

from loaders.registry import data_loaders, utilities

# Widen the help column so loader names and their descriptions stay on one line
# (the longest names, e.g. `mlb-statsapi-schedules`, overflow argparse's
# default 24-char column).
WideHelpFormatter = functools.partial(
    argparse.HelpFormatter, max_help_position=40, width=100
)


def main():
    parser = argparse.ArgumentParser(prog='buehrle')
    # Optional: bare `buehrle` (no subcommand) launches the interactive grid.
    subparsers = parser.add_subparsers(dest='command', metavar='<command>')

    load_parser = subparsers.add_parser('load', help='Run a data loader',
                                        formatter_class=WideHelpFormatter)
    load_subparsers = load_parser.add_subparsers(dest='loader', required=True, metavar='<loader>')
    for module in data_loaders():
        module.register(load_subparsers)

    for module in utilities():
        module.register(subparsers)

    args = parser.parse_args()
    if getattr(args, 'func', None) is None:
        from loaders.interactive.app import run
        raise SystemExit(run())
    args.func(args)


if __name__ == '__main__':
    main()
