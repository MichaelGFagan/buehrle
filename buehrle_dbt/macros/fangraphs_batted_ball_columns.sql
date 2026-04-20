{%- macro fangraphs_batted_ball_columns(stat) -%}

    {%- set columns = [
        {'raw_name': 'GB', 'column_name': 'ground_balls', 'type': 'int64'},
        {'raw_name': 'FB', 'column_name': 'fly_balls', 'type': 'int64'},
        {'raw_name': 'LD', 'column_name': 'line_drives', 'type': 'int64'},
        {'raw_name': 'IFFB', 'column_name': 'infield_fly_balls', 'type': 'int64'},
        {'raw_name': 'IFH', 'column_name': 'infield_hits', 'type': 'int64'},
        {'raw_name': 'BU', 'column_name': 'bunts', 'type': 'int64'},
        {'raw_name': 'BUH', 'column_name': 'bunt_hits', 'type': 'int64'},
        {'raw_name': 'Pull', 'column_name': 'pulled_balls', 'type': 'int64'},
        {'raw_name': 'Cent', 'column_name': 'centered_balls', 'type': 'int64'},
        {'raw_name': 'Oppo', 'column_name': 'opposite_field_balls', 'type': 'int64'},
        {'raw_name': 'Soft', 'column_name': 'soft_hit_balls', 'type': 'int64'},
        {'raw_name': 'Med', 'column_name': 'medium_hit_balls', 'type': 'int64'},
        {'raw_name': 'Hard', 'column_name': 'hard_hit_balls', 'type': 'int64'},
        {'raw_name': 'bipCount', 'column_name': 'balls_in_play_count', 'type': 'int64'},
        {'raw_name': 'Balls', 'column_name': 'balls_faced', 'type': 'int64'},
        {'raw_name': 'Strikes', 'column_name': 'strikes_faced', 'type': 'int64'},
        {'raw_name': 'Pitches', 'column_name': 'total_pitches_faced', 'type': 'int64'},
        {'raw_name': 'GB/FB', 'column_name':  'ground_ball_per_fly_ball',  'type':  'numeric'},
        {'raw_name': 'LD%',  'column_name': 'line_drive_percentage',  'type': 'numeric'},
        {'raw_name': 'GB%',  'column_name': 'ground_ball_percentage',  'type': 'numeric'},
        {'raw_name': 'FB%',  'column_name': 'fly_ball_percentage',  'type': 'numeric'},
        {'raw_name': 'IFFB%',  'column_name': 'infield_fly_ball_percentage',  'type': 'numeric'},
        {'raw_name': 'HR/FB',  'column_name': 'home_run_per_fly_ball',  'type': 'numeric'},
        {'raw_name': 'IFH%',  'column_name': 'infield_hit_percentage',  'type': 'numeric'},
        {'raw_name': 'BUH%',  'column_name': 'bunt_hit_percentage',  'type': 'numeric'},
        {'raw_name': 'Pull%',  'column_name': 'pulled_ball_percentage',  'type': 'numeric'},
        {'raw_name': 'Cent%',  'column_name': 'centered_ball_percentage',  'type': 'numeric'},
        {'raw_name': 'Oppo%',  'column_name': 'opposite_field_ball_percentage',  'type': 'numeric'},
        {'raw_name': 'Soft%',  'column_name': 'soft_hit_ball_percentage',  'type': 'numeric'},
        {'raw_name': 'Med%',  'column_name': 'medium_hit_ball_percentage',  'type': 'numeric'},
        {'raw_name': 'Hard%',  'column_name': 'hard_hit_ball_percentage',  'type': 'numeric'},
        {'raw_name': 'TTO%',  'column_name':  'total_zone_percentage',  'type':  'numeric'},
        {'raw_name': 'LD%+',  'column_name':  'line_drive_percentage_plus',  'type':  'int64'},
        {'raw_name': 'GB%+',  'column_name':  'ground_ball_percentage_plus',  'type':  'int64'},
        {'raw_name': 'FB%+',  'column_name':  'fly_ball_percentage_plus',  'type':  'int64'},
        {'raw_name': 'HRFB%+',  'column_name':  'home_run_per_fly_ball_plus',  'type':  'int64'},
        {'raw_name': 'Pull%+',  'column_name':  'pulled_ball_percentage_plus',  'type':  'int64'},
        {'raw_name': 'Cent%+',  'column_name':  'centered_ball_percentage_plus',  'type':  'int64'},
        {'raw_name': 'Oppo%+',  'column_name':  'opposite_field_ball_percentage_plus',  'type':  'int64'},
        {'raw_name': 'Soft%+',  'column_name':  'soft_hit_ball_percentage_plus',  'type':  'int64'},
        {'raw_name': 'Med%+',  'column_name':  'medium_hit_ball_percentage_plus',  'type':  'int64'},
        {'raw_name': 'Hard%+',  'column_name':  'hard_hit_ball_percentage_plus',  'type':  'int64'},
        {'raw_name': 'BABIP+',  'column_name':  'batting_average_on_balls_in_play_plus',  'type':  'int64'}
    ] -%}

    {# {%- if stat == 'pitching' -%}
        {%- set additional_columns = [
            {'raw_name': 'FBv', 'column_name': 'fastball_velocity', 'type': 'numeric'}
        ] -%}
    {%- else -%}
        {%- set additional_columns = [] -%}
    {%- endif -%}

    {%- set columns = columns + additional_columns -%} #}

    {{ return(columns) }}

{%- endmacro -%}