"""Unified entry point for buehrle loaders.

Usage:
    buehrle <loader> [args...]
    python -m loaders <loader> [args...]

Each loader module exposes a `register(subparsers)` function that adds its
subcommand and a `main(parser, args)` function that runs it. See
loaders/fangraphs/fangraphs.py for the reference implementation.
"""

import argparse

from loaders.registry import LOADERS


def main():
    parser = argparse.ArgumentParser(prog='buehrle')
    subparsers = parser.add_subparsers(dest='loader', required=True, metavar='<loader>')
    for module in LOADERS:
        module.register(subparsers)
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
