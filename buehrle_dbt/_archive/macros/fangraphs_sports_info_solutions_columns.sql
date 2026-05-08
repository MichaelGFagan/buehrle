{%- macro fangraphs_sports_info_solutions_columns(stat) -%}

    {%- set columns = [
        {'raw_name': 'FB%1', 'column_name': 'fastball_percentage_sis', 'type': 'numeric'},
        {'raw_name': 'SL%', 'column_name': 'slider_percentage_sis', 'type': 'numeric'},
        {'raw_name': 'CT%', 'column_name': 'cutter_percentage_sis', 'type': 'numeric'},
        {'raw_name': 'CB%', 'column_name': 'curveball_percentage_sis', 'type': 'numeric'},
        {'raw_name': 'CH%', 'column_name': 'changeup_percentage_sis', 'type': 'numeric'},
        {'raw_name': 'SF%', 'column_name': 'split_finger_fastaball_percentage_sis', 'type': 'numeric'},
        {'raw_name': 'KN%', 'column_name': 'knuckleball_percentage_sis', 'type': 'numeric'},
        {'raw_name': 'XX%', 'column_name': 'unidentified_pitch_percentage_sis', 'type': 'numeric'},
        {'raw_name': 'PO%', 'column_name': 'pitch_out_percentage_sis', 'type': 'numeric'},
        {'raw_name': 'FBv', 'column_name': 'fastball_velocity_sis', 'type': 'numeric'},
        {'raw_name': 'SLv', 'column_name': 'slider_velocity_sis', 'type': 'numeric'},
        {'raw_name': 'CTv', 'column_name': 'cutter_velocity_sis', 'type': 'numeric'},
        {'raw_name': 'CBv', 'column_name': 'curveball_velocity_sis', 'type': 'numeric'},
        {'raw_name': 'CHv', 'column_name': 'changeup_velocity_sis', 'type': 'numeric'},
        {'raw_name': 'SFv', 'column_name': 'split_finger_fastball_velocity_sis', 'type': 'numeric'},
        {'raw_name': 'KNv', 'column_name': 'knuckleball_velocity_sis', 'type': 'numeric'},
        {'raw_name': 'wFB',  'column_name':  'fastball_runs_above_average_sis',  'type':  'numeric'},
        {'raw_name': 'wSL',  'column_name':  'slider_runs_above_average_sis',  'type':  'numeric'},
        {'raw_name': 'wCT',  'column_name':  'cutter_runs_above_average_sis',  'type':  'numeric'},
        {'raw_name': 'wCB',  'column_name':  'curveball_runs_above_average_sis', 'type': 'numeric'},
        {'raw_name': 'wCH',  'column_name':  'changeup_runs_above_average_sis',  'type':  'numeric'},
        {'raw_name': 'wSF',  'column_name':  'split_finger_fastball_runs_above_average_sis',  'type':  'numeric'},
        {'raw_name': 'wKN',  'column_name':  'knuckleball_runs_above_average_sis',  'type':  'numeric'},
        {'raw_name': 'wFB/C',  'column_name':  'fastball_runs_above_average_per_100_pitches_sis',  'type':  'numeric'},
        {'raw_name': 'wSL/C',  'column_name':  'slider_runs_above_average_per_100_pitches_sis',  'type':  'numeric'},
        {'raw_name': 'wCT/C',  'column_name':  'cutter_runs_above_average_per_100_pitches_sis',  'type':  'numeric'},
        {'raw_name': 'wCB/C',  'column_name':  'curveball_runs_above_average_per_100_pitches_sis',  'type':  'numeric'},
        {'raw_name': 'wCH/C',  'column_name':  'changeup_runs_above_average_per_100_pitches_sis',  'type':  'numeric'},
        {'raw_name': 'wSF/C',  'column_name':  'split_finger_fastball_runs_above_average_per_100_pitches_sis',  'type':  'numeric'},
        {'raw_name': 'wKN/C',  'column_name':  'knuckleball_runs_above_average_per_100_pitches_sis',  'type':  'numeric'},
        {'raw_name': 'Swing%',  'column_name':  'swing_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'O-Swing%',  'column_name':  'outside_zone_swing_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'Z-Swing%',  'column_name':  'inside_zone_swing_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'Contact%',  'column_name':  'contact_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'O-Contact%',  'column_name':  'outside_zone_contact_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'Z-Contact%',  'column_name':  'inside_zone_contact_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'Zone%',  'column_name':  'pitches_seen_inside_zone_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'F-Strike%',  'column_name':  'first_pitch_strike_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'SwStr%',  'column_name':  'swinging_strike_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'CStr%',  'column_name':  'called_strike_percentage_sis',  'type':  'numeric'},
        {'raw_name': 'C+SwStr%',  'column_name':  'called_plus_swinging_strike_percentage_sis',  'type':  'numeric'}

    ] -%}

    {{ return(columns) }}

{%- endmacro -%}