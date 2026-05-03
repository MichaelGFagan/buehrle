import dlt
import duckdb
import pytest
import responses

from loaders.fangraphs import fangraphs as fg


@pytest.mark.parametrize('identifier, expected', [
    ('1B', '_1b'),
    ('-WPA', 'neg_wpa'),
    ('-RBI', 'neg_rbi'),
    ('WAROld', 'war_old'),
    ('C+SwStr%', 'c_plus_sw_str_pct'),
    ('xMLBAMID', 'mlbamid'),
    ('BB%', 'bb_pct'),
    ('wRC+', 'w_rc_plus'),
    ('K/9', 'k_per_9'),
    ('IFFB-', 'iffb_minus'),
    ('IFH%', 'ifh_pct'),
    ('PlayerName', 'player_name'),
    ('AVG', 'avg'),
    ('pfxFA-X', 'pfx_fa_x'),
    ('scFA-X', 'sc_fa_x'),
    ('piFA-X', 'pi_fa_x'),
])
def test_fangraphs_naming_convention(identifier, expected):
    nc = fg.FangraphsNamingConvention()
    assert nc.normalize_identifier(identifier) == expected


@pytest.mark.parametrize('playoff, expected', [
    (fg.FangraphsPlayoff.REGULAR_SEASON, 'regular season'),
    (fg.FangraphsPlayoff.WILD_CARD, 'Wild Card Series'),
    (fg.FangraphsPlayoff.DIVISION_SERIES, 'Division Series'),
    (fg.FangraphsPlayoff.LEAGUE_CHAMPIONSHIP_SERIES, 'League Championship Series'),
    (fg.FangraphsPlayoff.WORLD_SERIES, 'World Series'),
    (fg.FangraphsPlayoff.POSTSEASON, 'postseason'),
])
def test_fangraphs_playoff_str(playoff, expected):
    assert str(playoff) == expected


@pytest.mark.parametrize('playoff, expected', [
    (fg.FangraphsPlayoff.REGULAR_SEASON, 'REG'),
    (fg.FangraphsPlayoff.WILD_CARD, 'WC'),
    (fg.FangraphsPlayoff.DIVISION_SERIES, 'DS'),
    (fg.FangraphsPlayoff.LEAGUE_CHAMPIONSHIP_SERIES, 'CS'),
    (fg.FangraphsPlayoff.WORLD_SERIES, 'WS'),
    (fg.FangraphsPlayoff.POSTSEASON, 'POST'),
])
def test_fangraphs_playoff_abbreviation(playoff, expected):
    assert playoff.string_abbreviation() == expected


@pytest.mark.parametrize('stat, expected', [
    (fg.FangraphsStat.BATTING, 'batting'),
    (fg.FangraphsStat.PITCHING, 'pitching'),
    (fg.FangraphsStat.FIELDING, 'fielding'),
])
def test_fangraphs_stat_str(stat, expected):
    assert str(stat) == expected


@responses.activate
def test_fetch_season_returns_data_array():
    responses.add(
        responses.GET,
        fg.BASE_FANGRAPHS_URL,
        json={'data': [{'playerid': '1', 'Name': 'Trout'}, {'playerid': '2', 'Name': 'Judge'}]},
        status=200,
    )
    result = fg._fetch_season(fg.FangraphsStat.BATTING, 2024, fg.FangraphsPlayoff.REGULAR_SEASON)
    assert result == [{'playerid': '1', 'Name': 'Trout'}, {'playerid': '2', 'Name': 'Judge'}]


@responses.activate
def test_fetch_season_returns_empty_list_when_data_missing():
    responses.add(responses.GET, fg.BASE_FANGRAPHS_URL, json={}, status=200)
    assert fg._fetch_season(fg.FangraphsStat.BATTING, 2024, fg.FangraphsPlayoff.REGULAR_SEASON) == []


@responses.activate
def test_fetch_season_raises_on_non_2xx():
    responses.add(responses.GET, fg.BASE_FANGRAPHS_URL, status=500)
    with pytest.raises(Exception):
        fg._fetch_season(fg.FangraphsStat.BATTING, 2024, fg.FangraphsPlayoff.REGULAR_SEASON)


def _build_pipeline(tmp_path, name='fangraphs_test'):
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / 'test.duckdb')),
        dataset_name=name,
        pipelines_dir=str(tmp_path / 'dlt'),
    )


@responses.activate
def test_pipeline_loads_batting_data(tmp_path, monkeypatch):
    monkeypatch.setattr('time.sleep', lambda s: None)
    responses.add(
        responses.GET,
        fg.BASE_FANGRAPHS_URL,
        json={'data': [
            {'playerid': '100', 'Season': 2024, 'Team': 'NYY', 'Name': 'Mike Trout', 'WAR': 8.5},
        ]},
        status=200,
    )

    pipeline = _build_pipeline(tmp_path)
    source = fg.fangraphs(
        start_season=2024,
        end_season=2024,
        stats=(fg.FangraphsStat.BATTING,),
        postseason=(fg.FangraphsPlayoff.REGULAR_SEASON,),
    )
    pipeline.run(source)

    con = duckdb.connect(str(tmp_path / 'test.duckdb'))
    rows = con.execute(
        'SELECT playerid, name, war, leg, is_postseason FROM fangraphs_test.batting'
    ).fetchall()
    assert rows == [('100', 'Mike Trout', 8.5, 'REG', False)]
