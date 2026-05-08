{%- macro fangraphs_violations_columns(stat) -%}

    {%- set columns = [
        {'raw_name': 'PPTV', 'column_name': 'pitcher_pitch_timer_violations', 'type': 'numeric'},
        {'raw_name': 'CPTV', 'column_name': 'catcher_pitch_timer_violations', 'type': 'numeric'},
        {'raw_name': 'DSV', 'column_name': 'defensive_shift_violations', 'type': 'numeric'},
        {'raw_name': 'DGV', 'column_name': 'disengagement_violations', 'type': 'numeric'},
        {'raw_name': 'BPTV', 'column_name': 'batter_pitch_timer_violations', 'type': 'numeric'},
        {'raw_name': 'BTV', 'column_name': 'batter_timeout_violations', 'type': 'numeric'},
        {'raw_name': 'rPPTV', 'column_name': 'pitcher_pitch_timer_violations_runs', 'type': 'numeric'},
        {'raw_name': 'rCPTV', 'column_name': 'catcher_pitch_timer_violations_runs', 'type': 'numeric'},
        {'raw_name': 'rDSV', 'column_name': 'defensive_shift_violations_runs', 'type': 'numeric'},
        {'raw_name': 'rDGV', 'column_name': 'disengagement_violations_runs', 'type': 'numeric'},
        {'raw_name': 'rBPTV', 'column_name': 'batter_pitch_timer_violations_runs', 'type': 'numeric'},
        {'raw_name': 'rBTV', 'column_name': 'batter_timeout_violations_runs', 'type': 'numeric'},
        {'raw_name': 'EBV',  'column_name':  'total_violation_balls',  'type':  'numeric'},
        {'raw_name': 'ESV',  'column_name':  'total_violation_strikes',  'type':  'numeric'},
        {'raw_name': 'rFTeamV',  'column_name':  'total_opponent_violation_runs',  'type':  'numeric'},
        {'raw_name': 'rBTeamV',  'column_name':  'total_team_violation_runs',  'type':  'numeric'},
        {'raw_name': 'rTV',  'column_name':  'net_violation_runs',  'type':  'numeric'}, 
    ] -%}

    {{ return(columns) }}

{%- endmacro -%}
