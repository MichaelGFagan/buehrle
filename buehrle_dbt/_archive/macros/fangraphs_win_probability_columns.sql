{%- macro fangraphs_win_probability_columns(stat) -%}

    {%- set columns = [
        {'raw_name': 'WPA', 'column_name': 'win_probability_added', 'type': 'numeric'},
        {'raw_name': '-WPA', 'column_name': 'wpa_loss_advancement', 'type': 'numeric'},
        {'raw_name': '+WPA', 'column_name': 'wpa_win_advancement', 'type': 'numeric'},
        {'raw_name': 'RE24', 'column_name': 'run_expectancy_24', 'type': 'numeric'},
        {'raw_name': 'REW', 'column_name': 'run_expectancy_wins', 'type': 'numeric'},
        {'raw_name': 'pLI', 'column_name': 'average_leverage_index', 'type': 'numeric'},
        {'raw_name': 'WPA/LI', 'column_name': 'situational_wins', 'type': 'numeric'},
        {'raw_name': 'Clutch', 'column_name': 'clutch', 'type': 'numeric'}
    ] -%}

    {%- if stat == 'batting' -%}
        {%- set additional_columns = [
            {'raw_name': 'phLI', 'column_name': 'average_leverage_index_pinch_hitting', 'type': 'numeric'},
            {'raw_name': 'PH', 'column_name': 'pinch_hitting_opportunities', 'type': 'int64'}
        ] -%}
    {%- elif stat == 'pitching' -%}
        {%- set additional_columns = [
            {'raw_name': 'FBv', 'column_name': 'fastball_velocity', 'type': 'numeric'}
        ] -%}
    {%- else -%}
        {%- set additional_columns = [] -%}
    {%- endif -%}

    {%- set columns = columns + additional_columns -%}

    {{ return(columns) }}

{%- endmacro -%}