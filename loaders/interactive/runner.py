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
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor

from loaders.interactive.core import Job

DEFAULT_BASE_ARGV: tuple[str, ...] = (sys.executable, '-m', 'loaders')


def _run_one_job(
    job: Job,
    write: Callable[[str], None],
    base_argv: Sequence[str],
    env: dict[str, str],
) -> bool:
    """Run one job to completion, streaming its output via ``write``.

    Returns ``True`` on a zero exit, ``False`` otherwise. ``write`` must be
    thread-safe: the concurrent path calls this from worker threads.
    """
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
        write(f'[{job.label}] FAILED')
        return False
    return True


def _summarise(jobs: Sequence[Job], failures: list[str], write: Callable[[str], None]) -> None:
    """Emit the shared closing ``Done:`` / ``Failed:`` summary."""
    total = len(jobs)
    ok = total - len(failures)
    write('')
    write(f'Done: {ok}/{total} succeeded.')
    if failures:
        write(f'Failed: {", ".join(failures)}')


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
        if not _run_one_job(job, write, base_argv, env):
            failures.append(job.label)

    _summarise(jobs, failures, write)
    return failures


def stream_jobs_concurrent(
    jobs: Sequence[Job],
    write: Callable[[str], None],
    *,
    max_workers: int,
    base_argv: Sequence[str] = DEFAULT_BASE_ARGV,
) -> list[str]:
    """Run jobs concurrently on a bounded thread pool.

    Each job gets one ``_run_one_job`` on a worker thread; output lines from
    different jobs interleave, each tagged with its ``[label]`` prefix.
    ``write`` must be thread-safe (the TUI's ``call_from_thread`` and tests'
    ``list.append`` both are). Returns the failed jobs' labels in submission
    order, and emits the same closing summary as :func:`stream_jobs`.
    """
    env = {**os.environ, 'PYTHONUNBUFFERED': '1'}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(
            lambda job: (job.label, _run_one_job(job, write, base_argv, env)),
            jobs,
        ))

    failures = [label for label, ok in results if not ok]
    _summarise(jobs, failures, write)
    return failures


def run_jobs(
    jobs: Sequence[Job],
    write: Callable[[str], None],
    *,
    max_workers: int,
    base_argv: Sequence[str] = DEFAULT_BASE_ARGV,
) -> list[str]:
    """Dispatch to the sequential or concurrent runner by ``max_workers``.

    ``max_workers <= 1`` runs sequentially; anything higher runs on a bounded
    pool. The concurrency gate itself lives in
    :func:`loaders.interactive.core.worker_count`.
    """
    if max_workers <= 1:
        return stream_jobs(jobs, write, base_argv=base_argv)
    return stream_jobs_concurrent(jobs, write, max_workers=max_workers, base_argv=base_argv)
