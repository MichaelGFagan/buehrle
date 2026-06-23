"""Subprocess streaming for the interactive TUI.

Separated from :mod:`loaders.interactive.app` (the Textual shell) so the real
IO logic — spawning ``python -m loaders ...``, line-streaming its merged
stdout/stderr, normalising progress ``\\r``, and continue-on-error accounting —
is unit-testable rather than buried in untestable UI code.

It is synchronous and runs on a Textual *thread* worker (see ``app.py``); the
worker forwards each line to the UI with ``call_from_thread`` so the event loop
never blocks. The TUI passes that thread-safe writer as ``write``; tests pass a
list's ``append``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Callable, Sequence

from loaders.interactive.core import Job

DEFAULT_BASE_ARGV: tuple[str, ...] = (sys.executable, '-m', 'loaders')


def stream_jobs(
    jobs: Sequence[Job],
    write: Callable[[str], None],
    base_argv: Sequence[str] = DEFAULT_BASE_ARGV,
) -> list[str]:
    """Run each job sequentially, emitting every output line via ``write``.

    Continues past a failing job; returns the labels of the jobs that exited
    non-zero. ``base_argv`` is the command prefix each job's ``argv_tail`` is
    appended to (injectable for tests).
    """
    failures: list[str] = []
    env = {**os.environ, 'PYTHONUNBUFFERED': '1'}

    for job in jobs:
        write('')
        write(f'=== {" ".join(job.argv_tail)} ===')
        proc = subprocess.Popen(
            [*base_argv, *job.argv_tail],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            # dlt/logging progress uses '\r'; split so each chunk is one clean
            # line in an append-only log.
            for piece in line.replace('\r', '\n').splitlines():
                write(f'[{job.label}] {piece}')
        if proc.wait() != 0:
            failures.append(job.label)
            write(f'[{job.label}] FAILED')

    total = len(jobs)
    ok = total - len(failures)
    write('')
    write(f'Done: {ok}/{total} succeeded.')
    if failures:
        write(f'Failed: {", ".join(failures)}')
    return failures
