import json
import re
import sys

import pandas as pd
import pytest
import requests
import responses

import loaders.__main__ as loaders_main
from loaders.baseball_reference import baseball_reference_draft_results as draft

REAL_DRAFT_YEARS = dict(draft.DRAFT_YEARS)  # capture before any fixture patches it

DRAFT_YEARS_FIXTURE = {
    'test_type': {
        'draft_lengths': [
            [2000, 2002, 30],
            [2003, 2003, 40],
            [2005, 2010, 50],
        ],
    },
}


@pytest.fixture(autouse=True)
def patched_draft_years(monkeypatch):
    monkeypatch.setattr(draft, 'DRAFT_YEARS', DRAFT_YEARS_FIXTURE)


@pytest.fixture
def real_draft_years(monkeypatch):
    """Override the autouse patched_draft_years with real values — needed for main tests
    that mock URLs against real draft types like 'junreg'."""
    monkeypatch.setattr(draft, 'DRAFT_YEARS', REAL_DRAFT_YEARS)


@pytest.mark.parametrize('year, expected', [
    (1999, 0),
    (2000, 30),
    (2001, 30),
    (2002, 30),
    (2003, 40),
    (2004, 0),
    (2005, 50),
    (2010, 50),
    (2011, 0),
])
def test_rounds_for_year(year, expected):
    assert draft._rounds_for_year('test_type', year) == expected


def test_clean_with_links_major_league_player():
    df = pd.DataFrame({
        'Name': [('John Doe', '/players/d/doejo01.shtml')],
        'Tm': [('NYY', '?team_ID=NYY&year_ID=2024')],
        'G': [('15', None)],
    })
    result = draft._clean_with_links(df)
    row = result.iloc[0]
    assert row['link'] == '/players/d/doejo01.shtml'
    assert row['id_type'] == 'baseball_reference_id'
    assert row['player_id'] == 'doejo01'
    assert row['notes'] == 'NA'
    assert row['team_id'] == 'NYY'
    assert row['G'] == '15'


def test_clean_with_links_minor_league_player():
    df = pd.DataFrame({
        'Name': [('Jane Doe', 'register?id=doe123')],
        'Tm': [('NYY', '?team_ID=NYY&year_ID=2024')],
    })
    result = draft._clean_with_links(df)
    row = result.iloc[0]
    assert row['id_type'] == 'baseball_reference_minor_league_id'
    assert row['player_id'] == 'doe123'


def test_clean_with_links_no_links_extracts_notes_and_returns_na():
    df = pd.DataFrame({
        'Name': [('John (DNS)', None)],
        'Tm': [('NYY', None)],
    })
    result = draft._clean_with_links(df)
    row = result.iloc[0]
    assert row['link'] == 'NA'
    assert row['id_type'] == 'NA'
    assert row['player_id'] == 'NA'
    assert row['notes'] == 'DNS'
    assert row['team_id'] == 'NA'


@pytest.fixture
def no_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr('time.sleep', lambda s: sleeps.append(s))
    return sleeps


def test_fetch_round_with_retry_first_attempt_succeeds(monkeypatch, no_sleep):
    monkeypatch.setattr(draft, '_fetch_round', lambda dt, y, r: 'success')
    assert draft._fetch_round_with_retry('junreg', 2024, 1) == 'success'
    assert no_sleep == []


def test_fetch_round_with_retry_succeeds_after_connection_error(monkeypatch, no_sleep):
    attempts = []
    def fake_fetch(dt, y, r):
        attempts.append(1)
        if len(attempts) == 1:
            raise requests.exceptions.ConnectionError('boom')
        return 'success'
    monkeypatch.setattr(draft, '_fetch_round', fake_fetch)

    assert draft._fetch_round_with_retry('junreg', 2024, 1) == 'success'
    assert no_sleep == [30]


def test_fetch_round_with_retry_succeeds_after_timeout(monkeypatch, no_sleep):
    attempts = []
    def fake_fetch(dt, y, r):
        attempts.append(1)
        if len(attempts) == 1:
            raise requests.exceptions.Timeout('slow')
        return 'success'
    monkeypatch.setattr(draft, '_fetch_round', fake_fetch)

    assert draft._fetch_round_with_retry('junreg', 2024, 1) == 'success'


def test_fetch_round_with_retry_exhausts_retries_and_raises_last(monkeypatch, no_sleep):
    monkeypatch.setattr(
        draft, '_fetch_round',
        lambda dt, y, r: (_ for _ in ()).throw(requests.exceptions.ConnectionError('persistent')),
    )

    with pytest.raises(requests.exceptions.ConnectionError):
        draft._fetch_round_with_retry('junreg', 2024, 1)

    assert no_sleep == [30, 60, 120]


def test_fetch_round_with_retry_does_not_retry_on_other_exceptions(monkeypatch, no_sleep):
    attempts = []
    def fake_fetch(dt, y, r):
        attempts.append(1)
        raise requests.exceptions.HTTPError('500')
    monkeypatch.setattr(draft, '_fetch_round', fake_fetch)

    with pytest.raises(requests.exceptions.HTTPError):
        draft._fetch_round_with_retry('junreg', 2024, 1)

    assert len(attempts) == 1
    assert no_sleep == []


def test_draft_type_str():
    assert str(draft.DraftType.JUNREG) == 'junreg'
    assert str(draft.DraftType.AUGLEG) == 'augleg'


@responses.activate
def test_fetch_round_returns_none_when_no_rows_match_year():
    html = (
        '<table>'
        '<thead><tr><th>Year</th><th>Rnd</th><th>RdPck</th><th>Name</th><th>Tm</th></tr></thead>'
        '<tbody><tr><td>1999</td><td>1</td><td>1</td><td>Old Player</td><td>NYY</td></tr></tbody>'
        '</table>'
    )
    responses.add(responses.GET, re.compile(r'.*'), body=html, status=200)
    assert draft._fetch_round('junreg', 2024, 1) is None


@responses.activate
def test_main_with_failed_rounds(monkeypatch, fake_make_pipeline, real_draft_years, capsys):
    # 500 response — _fetch_round raises HTTPError, retry treats it as non-retriable, resource catches it
    responses.add(responses.GET, re.compile(r'https://www\.baseball-reference\.com/draft/.*'), status=500)

    monkeypatch.setattr('time.sleep', lambda s: None)
    monkeypatch.setattr(draft, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'buehrle', 'load', 'baseball-reference-draft',
        '--season', '2024',
        '--rounds', json.dumps({'junreg': [1]}),
    ])
    loaders_main.main()

    captured = capsys.readouterr()
    assert 'Failed rounds:' in captured.out


@responses.activate
def test_main_with_draft_types_flag(monkeypatch, fake_make_pipeline, real_draft_years):
    html = (
        '<table>'
        '<thead><tr><th>Year</th><th>Rnd</th><th>RdPck</th><th>Name</th><th>Tm</th></tr></thead>'
        '<tbody><tr><td>2024</td><td>1</td><td>1</td>'
        '<td><a href="/players/d/doejo01.shtml">John Doe</a></td>'
        '<td><a href="?team_ID=NYY&year_ID=2024">NYY</a></td></tr></tbody>'
        '</table>'
    )
    responses.add(responses.GET, re.compile(r'.*'), body=html, status=200)

    monkeypatch.setattr('time.sleep', lambda s: None)
    monkeypatch.setattr(draft, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'buehrle', 'load', 'baseball-reference-draft',
        '--season', '2024',
        '--draft-types', 'junreg',
    ])
    loaders_main.main()


@responses.activate
def test_main_with_all_draft_types_flag(monkeypatch, fake_make_pipeline, real_draft_years):
    html = (
        '<table>'
        '<thead><tr><th>Year</th><th>Rnd</th><th>RdPck</th><th>Name</th><th>Tm</th></tr></thead>'
        '<tbody><tr><td>2024</td><td>1</td><td>1</td>'
        '<td><a href="/players/d/doejo01.shtml">John Doe</a></td>'
        '<td><a href="?team_ID=NYY&year_ID=2024">NYY</a></td></tr></tbody>'
        '</table>'
    )
    responses.add(responses.GET, re.compile(r'.*'), body=html, status=200)

    monkeypatch.setattr('time.sleep', lambda s: None)
    monkeypatch.setattr(draft, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'buehrle', 'load', 'baseball-reference-draft',
        '--season', '2024',
        '--all-draft-types',
    ])
    loaders_main.main()


@responses.activate
def test_main_executes(monkeypatch, fake_make_pipeline, real_draft_years):
    html = (
        '<table>'
        '<thead><tr><th>Year</th><th>Rnd</th><th>RdPck</th><th>Name</th><th>Tm</th></tr></thead>'
        '<tbody><tr>'
        '<td>2024</td><td>1</td><td>1</td>'
        '<td><a href="/players/d/doejo01.shtml">John Doe</a></td>'
        '<td><a href="?team_ID=NYY&year_ID=2024">NYY</a></td>'
        '</tr></tbody>'
        '</table>'
    )
    responses.add(
        responses.GET,
        'https://www.baseball-reference.com/draft/?year_ID=2024&draft_round=1&draft_type=junreg&query_type=year_round',
        body=html,
        status=200,
    )

    monkeypatch.setattr('time.sleep', lambda s: None)
    monkeypatch.setattr(draft, 'make_pipeline', fake_make_pipeline)
    monkeypatch.setattr(sys, 'argv', [
        'buehrle', 'load', 'baseball-reference-draft',
        '--season', '2024',
        '--rounds', json.dumps({'junreg': [1]}),
        '--full-refresh',
    ])
    loaders_main.main()
