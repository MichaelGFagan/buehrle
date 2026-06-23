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
