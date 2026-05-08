{%- macro fangraphs_context_columns(stat) -%}

    {%- set columns = [
        {'raw_name': 'playerid', 'column_name': 'fangraphs_id', 'type': 'int64'},
        {'raw_name': 'xMLBAMID', 'column_name': 'mlbam_id', 'type': 'int64'},
        {'raw_name': 'Season', 'column_name': 'year_id', 'type': 'int64'},
        {'raw_name': 'teamid', 'column_name': 'team_id', 'type': 'varchar'},
        {'raw_name': 'leg', 'column_name': 'leg', 'type': 'varchar'},
        {'raw_name': 'is_postseason', 'column_name': 'is_postseason', 'type': 'boolean'},
        {'raw_name': 'PlayerNameRoute', 'column_name': 'name', 'type': 'varchar'},
        {'raw_name': 'PlayerName', 'column_name': 'name_accented', 'type': 'varchar'},
        {'raw_name': 'TeamName', 'column_name': 'team', 'type': 'varchar'},
        {'raw_name': 'Age', 'column_name': 'age', 'type': 'int64'},
        {'raw_name': 'AgeR', 'column_name': 'age_range', 'type': 'varchar'},
        {'raw_name': 'Position', 'column_name': 'position', 'type': 'varchar'}
    ] -%}

    {%- if stat == 'batting' -%}
        {%- set additional_columns = [
            {'raw_name': 'Bats', 'column_name': 'bats', 'type': 'varchar'}
        ] -%}
    {%- elif stat == 'pitching' -%}
        {%- set additional_columns = [
            {'raw_name': 'Throws', 'column_name': 'throws', 'type': 'varchar'}
        ] -%}
    {%- else -%}
        {%- set additional_columns = [] -%}
    {%- endif -%}

    {%- set columns = columns + additional_columns -%}

    {{ return(columns) }}

{%- endmacro -%}