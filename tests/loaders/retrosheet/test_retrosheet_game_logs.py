import pyarrow as pa

from loaders.retrosheet.retrosheet_game_logs import _add_game_type


def test_add_game_type_appends_utf8_column_with_value():
    table = pa.table({'home_team': ['NYY', 'BOS']})
    result = _add_game_type(table, 'regular_season')
    assert result.column_names == ['home_team', 'game_type']
    assert result.schema.field('game_type').type == pa.utf8()
    assert result.column('game_type').to_pylist() == ['regular_season', 'regular_season']
    assert result.column('home_team').to_pylist() == ['NYY', 'BOS']


def test_add_game_type_empty_table():
    table = pa.table({'home_team': pa.array([], type=pa.utf8())})
    result = _add_game_type(table, 'world_series')
    assert result.num_rows == 0
    assert result.column('game_type').to_pylist() == []
