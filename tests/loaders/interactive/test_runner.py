"""Tests for the interactive subprocess streamer (loaders/interactive/runner.py).

Uses ``python -c <code>`` as the base argv so each Job runs a fast, hermetic
script instead of a real loader.
"""

import sys

from loaders.interactive.core import Job
from loaders.interactive.runner import stream_jobs

PY = (sys.executable, '-c')


def _run(jobs):
    lines: list[str] = []
    failures = stream_jobs(jobs, lines.append, base_argv=PY)
    return failures, lines


def test_streams_output_and_reports_success():
    job = Job(label='ok', argv_tail=['print("hello world")'])
    failures, lines = _run([job])
    assert failures == []
    assert '[ok] hello world' in lines
    assert any(line.startswith('=== ') for line in lines)
    assert 'Done: 1/1 succeeded.' in lines


def test_nonzero_exit_is_recorded_but_does_not_stop_the_run():
    failing = Job(label='bad', argv_tail=['import sys; print("oops"); sys.exit(3)'])
    after = Job(label='next', argv_tail=['print("ran anyway")'])
    failures, lines = _run([failing, after])
    assert failures == ['bad']
    assert '[bad] oops' in lines
    assert '[bad] FAILED' in lines
    assert '[next] ran anyway' in lines      # continued past the failure
    assert 'Done: 1/2 succeeded.' in lines
    assert 'Failed: bad' in lines


def test_carriage_returns_split_into_separate_lines():
    # dlt-style progress: '\r' separates redraws on one physical line.
    job = Job(label='p', argv_tail=[r'print("a\rb\rc")'])
    _, lines = _run([job])
    assert '[p] a' in lines and '[p] b' in lines and '[p] c' in lines


def test_multiple_jobs_run_in_order():
    jobs = [
        Job(label='one', argv_tail=['print(1)']),
        Job(label='two', argv_tail=['print(2)']),
    ]
    failures, lines = _run(jobs)
    assert failures == []
    assert lines.index('[one] 1') < lines.index('[two] 2')
    assert 'Done: 2/2 succeeded.' in lines


def test_empty_job_list():
    failures, lines = _run([])
    assert failures == []
    assert 'Done: 0/0 succeeded.' in lines


# --- concurrent path -------------------------------------------------------

import threading  # noqa: E402

from loaders.interactive.runner import run_jobs, stream_jobs_concurrent  # noqa: E402


class _LockedList:
    """A thread-safe list-of-lines writer, mimicking the TUI's serialised
    ``call_from_thread`` writes."""

    def __init__(self):
        self._lines: list[str] = []
        self._lock = threading.Lock()

    def append(self, line: str) -> None:
        with self._lock:
            self._lines.append(line)

    @property
    def lines(self) -> list[str]:
        return self._lines


def test_concurrent_runs_every_job_and_collects_failures():
    jobs = [
        Job(label='ok1', argv_tail=['print("a")']),
        Job(label='bad', argv_tail=['import sys; print("boom"); sys.exit(1)']),
        Job(label='ok2', argv_tail=['print("b")']),
    ]
    sink = _LockedList()
    failures = stream_jobs_concurrent(jobs, sink.append, max_workers=3, base_argv=PY)
    assert failures == ['bad']
    assert '[ok1] a' in sink.lines
    assert '[ok2] b' in sink.lines
    assert '[bad] boom' in sink.lines
    assert '[bad] FAILED' in sink.lines
    assert 'Done: 2/3 succeeded.' in sink.lines
    assert 'Failed: bad' in sink.lines


def test_concurrent_actually_interleaves():
    # Two jobs that each print, sleep, print — under real concurrency the second
    # job's first line lands before the first job's second line.
    jobs = [
        Job(label='one', argv_tail=['import time; print("1a"); time.sleep(0.3); print("1b")']),
        Job(label='two', argv_tail=['import time; print("2a"); time.sleep(0.3); print("2b")']),
    ]
    sink = _LockedList()
    stream_jobs_concurrent(jobs, sink.append, max_workers=2, base_argv=PY)
    lines = sink.lines
    # both second-lines come after both first-lines => they ran in parallel.
    assert max(lines.index('[one] 1a'), lines.index('[two] 2a')) < \
        min(lines.index('[one] 1b'), lines.index('[two] 2b'))


def test_run_jobs_dispatches_sequential_for_single_worker():
    jobs = [Job(label='x', argv_tail=['print("hi")'])]
    lines: list[str] = []
    failures = run_jobs(jobs, lines.append, max_workers=1, base_argv=PY)
    assert failures == []
    assert '[x] hi' in lines


def test_run_jobs_dispatches_concurrent_for_multiple_workers():
    jobs = [
        Job(label='a', argv_tail=['print("A")']),
        Job(label='b', argv_tail=['print("B")']),
    ]
    sink = _LockedList()
    failures = run_jobs(jobs, sink.append, max_workers=2, base_argv=PY)
    assert failures == []
    assert '[a] A' in sink.lines and '[b] B' in sink.lines
    assert 'Done: 2/2 succeeded.' in sink.lines
