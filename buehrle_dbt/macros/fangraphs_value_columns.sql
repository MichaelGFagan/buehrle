{%- macro fangraphs_value_columns(stat) -%}

    {%- if stat == 'batting' -%}
        {%- set columns = [
            {'raw_name': 'UBR', 'column_name': 'ultimate_base_running', 'type': 'numeric'},
            {'raw_name': 'GDPRuns', 'column_name': 'grounded_into_double_play_runs', 'type': 'numeric'},
            {'raw_name': 'wBsR', 'column_name': 'weighted_base_running', 'type': 'numeric'},
            {'raw_name': 'wRC', 'column_name': 'weighted_runs_created', 'type': 'numeric'},
            {'raw_name': 'wRAA', 'column_name': 'weighted_runs_above_average', 'type': 'numeric'},
            {'raw_name': 'wOBA', 'column_name': 'weighted_on_base_average', 'type': 'numeric'},
            {'raw_name': 'wRC+', 'column_name': 'weighted_runs_created_plus', 'type': 'int64'},
            {'raw_name': 'Batting', 'column_name': 'batting_runs_above_average', 'type': 'numeric'},
            {'raw_name': 'BaseRunning', 'column_name': 'base_running_runs_above_average', 'type': 'numeric'},
            {'raw_name': 'Fielding', 'column_name': 'fielding_runs_above_average', 'type': 'numeric'},
            {'raw_name': 'CFraming', 'column_name': 'catcher_framing_runs_above_average', 'type': 'numeric'},
            {'raw_name': 'Positional', 'column_name': 'positional_runs_adjustment', 'type': 'numeric'},
            {'raw_name': 'Offense', 'column_name': 'offense_runs_above_average', 'type': 'numeric'},
            {'raw_name': 'Defense', 'column_name': 'defense_runs_above_average', 'type': 'numeric'},
            {'raw_name': 'RAR', 'column_name': 'runs_above_replacement', 'type': 'numeric'},
            {'raw_name': 'wLeague', 'column_name': 'league_zero_out_adjustment', 'type': 'numeric'},
            {'raw_name': 'Replacement', 'column_name': 'replacement_runs', 'type': 'numeric'},
            {'raw_name': 'WAR', 'column_name': 'wins_above_replacement', 'type': 'numeric'},
            {'raw_name': 'WAROld', 'column_name': 'wins_above_replacement_old', 'type': 'numeric'},
            {'raw_name': 'Dollars', 'column_name': 'wins_above_replacement_dollar_value', 'type': 'numeric'}
        ] -%}
    {%- elif stat == 'pitching' -%}
        {%- set additional_columns = [
            {'raw_name': 'FBv', 'column_name': 'fastball_velocity', 'type': 'numeric'}
        ] -%}
    {%- else -%}
        {%- set additional_columns = [] -%}
    {%- endif -%}

    {# {%- set columns = columns + additional_columns -%} #}

    {{ return(columns) }}

{%- endmacro -%}