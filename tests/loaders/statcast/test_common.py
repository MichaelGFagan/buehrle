import polars as pl
import pytest
import requests
import responses

from loaders.statcast._common import REQUEST_TIMEOUT, USER_AGENT, fetch_csv, inject_labels

URL = 'https://example.com/data.csv'


def test_inject_labels_adds_new_column_as_string():
    df = pl.DataFrame({'player_id': ['a', 'b']})
    result = inject_labels(df, {'year': 2024})
    assert result.columns == ['player_id', 'year']
    assert result['year'].to_list() == ['2024', '2024']


def test_inject_labels_preserves_existing_column():
    df = pl.DataFrame({'year': ['2023', '2023']})
    result = inject_labels(df, {'year': 2024})
    assert result['year'].to_list() == ['2023', '2023']


def test_inject_labels_handles_mix_of_new_and_existing():
    df = pl.DataFrame({'team': ['NYY', 'BOS']})
    result = inject_labels(df, {'year': 2024, 'team': 'LAD'})
    assert result['team'].to_list() == ['NYY', 'BOS']
    assert result['year'].to_list() == ['2024', '2024']


def test_inject_labels_empty_dict_returns_df_unchanged():
    df = pl.DataFrame({'player_id': ['a', 'b']})
    result = inject_labels(df, {})
    assert result.columns == ['player_id']
    assert result.to_dicts() == df.to_dicts()


@responses.activate
def test_fetch_csv_returns_dataframe_for_non_empty_response():
    responses.add(responses.GET, URL, body='id,year\n1,2024\n2,2024', status=200)
    df = fetch_csv(URL)
    assert df is not None
    assert df.height == 2
    assert df.columns == ['id', 'year']


@responses.activate
def test_fetch_csv_returns_none_for_header_only_response():
    responses.add(responses.GET, URL, body='id,year\n', status=200)
    assert fetch_csv(URL) is None


@responses.activate
def test_fetch_csv_raises_on_non_2xx():
    responses.add(responses.GET, URL, status=404)
    with pytest.raises(requests.exceptions.HTTPError):
        fetch_csv(URL)


@responses.activate
def test_fetch_csv_sends_user_agent_header():
    responses.add(responses.GET, URL, body='id\n1', status=200)
    fetch_csv(URL)
    assert responses.calls[0].request.headers['User-Agent'] == USER_AGENT


def test_fetch_csv_uses_timeout(monkeypatch):
    captured = {}

    class FakeResponse:
        content = b'id\n1'
        def raise_for_status(self):
            pass

    def fake_get(url, **kwargs):
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr('loaders.statcast._common.requests.get', fake_get)
    fetch_csv(URL)
    assert captured['timeout'] == REQUEST_TIMEOUT
