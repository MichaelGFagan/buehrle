{%- macro fangraphs_standard_and_advanced_columns(stat) -%}

    {%- if stat == 'batting' -%}
        {%- set columns = [
            {'raw_name': 'G', 'column_name': 'games', 'type': 'int64'},
            {'raw_name': 'TG', 'column_name': 'possible_games', 'type': 'int64'},
            {'raw_name': 'AB', 'column_name': 'at_bats', 'type': 'int64'},
            {'raw_name': 'PA', 'column_name': 'plate_appearances', 'type': 'int64'},
            {'raw_name': 'H', 'column_name': 'hits', 'type': 'int64'},
            {'raw_name': '1B', 'column_name': 'singles', 'type': 'int64'},
            {'raw_name': '2B', 'column_name': 'doubles', 'type': 'int64'},
            {'raw_name': '3B', 'column_name': 'triples', 'type': 'int64'},
            {'raw_name': 'HR', 'column_name': 'home_runs', 'type': 'int64'},
            {'raw_name': 'R', 'column_name': 'runs', 'type': 'int64'},
            {'raw_name': 'RBI', 'column_name': 'runs_batted_in', 'type': 'int64'},
            {'raw_name': 'BB', 'column_name': 'base_on_balls', 'type': 'int64'},
            {'raw_name': 'IBB', 'column_name': 'intentional_walks', 'type': 'int64'},
            {'raw_name': 'SO', 'column_name': 'strikeouts', 'type': 'int64'},
            {'raw_name': 'HBP', 'column_name': 'hit_by_pitch', 'type': 'int64'},
            {'raw_name': 'SF', 'column_name': 'sacrifice_flies', 'type': 'int64'},
            {'raw_name': 'SH', 'column_name': 'sacrifice_hits', 'type': 'int64'},
            {'raw_name': 'GDP', 'column_name': 'grounded_into_double_play', 'type': 'int64'},
            {'raw_name': 'SB', 'column_name': 'stolen_bases', 'type': 'int64'},
            {'raw_name': 'CS', 'column_name': 'caught_stealing', 'type': 'int64'},
            {'raw_name': 'AVG', 'column_name': 'batting_average', 'type': 'numeric'},
            {'raw_name': 'BB%', 'column_name': 'walk_percentage', 'type': 'numeric'},
            {'raw_name': 'K%', 'column_name': 'strikeout_percentage', 'type': 'numeric'},
            {'raw_name': 'BB/K', 'column_name': 'walks_per_strikeout', 'type': 'numeric'},
            {'raw_name': 'SLG', 'column_name': 'slugging_percentage', 'type': 'numeric'},
            {'raw_name': 'OBP', 'column_name': 'on_base_percentage', 'type': 'numeric'},
            {'raw_name': 'OPS', 'column_name': 'ops', 'type': 'numeric'},
            {'raw_name': 'ISO', 'column_name': 'isolated_power', 'type': 'numeric'},
            {'raw_name': 'Spd', 'column_name': 'speed_score', 'type': 'numeric'},
            {'raw_name': 'BABIP', 'column_name': 'batting_average_on_balls_in_play', 'type': 'numeric'},
            {'raw_name': 'AVG+', 'column_name': 'batting_average_plus', 'type': 'int64'},
            {'raw_name': 'BB+', 'column_name': 'walk_percentage_plus', 'type': 'int64'},
            {'raw_name': 'K+', 'column_name': 'strikeout_percentage_plus', 'type': 'int64'},
            {'raw_name': 'OBP+', 'column_name': 'on_base_percentage_plus', 'type': 'int64'},
            {'raw_name': 'SLG+', 'column_name': 'slugging_percentage_plus', 'type': 'int64'},
            {'raw_name': 'ISO+', 'column_name': 'isolated_power_plus', 'type': 'int64'}
        ] -%}
    {%- elif stat == 'pitching' -%}
        {%- set columns = [
            {'raw_name': 'G', 'column_name': 'games', 'type': 'int64'},
            {'raw_name': 'TG', 'column_name': 'possible_games', 'type': 'int64'},
            {'raw_name': 'GS', 'column_name': 'games_started', 'type': 'int64'},
            {'raw_name': 'QS', 'column_name': 'quality_starts', 'type': 'int64'},
            {'raw_name': 'CG', 'column_name': 'complete_games', 'type': 'int64'},
            {'raw_name': 'W', 'column_name': 'wins', 'type': 'int64'},
            {'raw_name': 'L', 'column_name': 'losses', 'type': 'int64'},
            {'raw_name': 'HLD', 'column_name': 'holds', 'type': 'int64'},
            {'raw_name': 'SV', 'column_name': 'saves', 'type': 'int64'},
            {'raw_name': 'BS', 'column_name': 'blown_saves', 'type': 'int64'},
            {'raw_name': 'ER', 'column_name': 'earned_runs', 'type': 'int64'},
            {'raw_name': 'ERA', 'column_name': 'earned_run_average', 'type': 'numeric'},
            {'raw_name': 'ShO', 'column_name': 'shutouts', 'type': 'int64'},
            {'raw_name': 'IP', 'column_name': 'innings_pitched', 'type': 'numeric'},
            {'raw_name': 'TBF', 'column_name': 'batters_faced', 'type': 'int64'},
            {'raw_name': 'H', 'column_name': 'hits', 'type': 'int64'},
            {'raw_name': 'HR', 'column_name': 'home_runs', 'type': 'int64'},
            {'raw_name': 'R', 'column_name': 'runs', 'type': 'int64'},
            {'raw_name': 'RS', 'column_name': 'run_support', 'type': 'int64'},
            {'raw_name': 'SO', 'column_name': 'strikeouts', 'type': 'int64'},
            {'raw_name': 'BB', 'column_name': 'walks', 'type': 'int64'},
            {'raw_name': 'IBB', 'column_name': 'intentional_walks', 'type': 'int64'},
            {'raw_name': 'HBP', 'column_name': 'hit_by_pitch', 'type': 'int64'},
            {'raw_name': 'WP', 'column_name': 'wild_pitches', 'type': 'int64'},
            {'raw_name': 'BK', 'column_name': 'balks', 'type': 'int64'},

            {'raw_name': 'FIP', 'column_name': 'fielding_independent_pitching', 'type': 'numeric'},
            {'raw_name': 'kwERA', 'column_name': 'strikeout_walk_earned_run_average', 'type': 'numeric'},
            {'raw_name': 'SIERA', 'column_name': 'skill_interactive_earned_run_average', 'type': 'numeric'},
            {'raw_name': 'E-F', 'column_name': 'earned_run_average_minus_fielding_independent_pitching', 'type': 'numeric'},

            {'raw_name': 'RS/9', 'column_name': 'run_support_per_nine_innings', 'type': 'numeric'},
            {'raw_name': 'K/9', 'column_name': 'strikeouts_per_nine_innings', 'type': 'numeric'},
            {'raw_name': 'BB/9', 'column_name': 'walks_per_nine_innings', 'type': 'numeric'},
            {'raw_name': 'K/BB', 'column_name': 'strikeouts_per_walk', 'type': 'numeric'},
            {'raw_name': 'H/9', 'column_name': 'hits_per_nine_innings', 'type': 'numeric'},
            {'raw_name': 'HR/9', 'column_name': 'home_runs_per_nine_innings', 'type': 'numeric'},
            {'raw_name': 'K%', 'column_name': 'strikeout_percentage', 'type': 'numeric'},
            {'raw_name': 'BB%', 'column_name': 'walk_percentage', 'type': 'numeric'},
            {'raw_name': 'K-BB%', 'column_name': 'strikeout_minus_walk_percentage', 'type': 'numeric'},
            {'raw_name': 'AVG', 'column_name': 'batting_average_against', 'type': 'numeric'},
            {'raw_name': 'WHIP', 'column_name': 'walks_and_hits_per_inning_pitched', 'type': 'numeric'},
            {'raw_name': 'LOB%', 'column_name': 'left_on_base_percentage', 'type': 'numeric'},            
            
            {'raw_name': 'K/9+', 'column_name': 'strikeouts_per_nine_innings_plus', 'type': 'int64'},
            {'raw_name': 'BB/9+', 'column_name': 'walks_per_nine_innings_plus', 'type': 'int64'},
            {'raw_name': 'H/9+', 'column_name': 'hits_per_nine_innings_plus', 'type': 'int64'},
            {'raw_name': 'HR/9+', 'column_name': 'home_runs_per_nine_innings_plus', 'type': 'int64'},
            {'raw_name': 'AVG+', 'column_name': 'batting_average_against_plus', 'type': 'int64'},
            {'raw_name': 'WHIP+', 'column_name': 'walks_and_hits_per_inning_pitched_plus', 'type': 'int64'},
            {'raw_name': 'LOB%+', 'column_name': 'left_on_base_percentage_plus', 'type': 'int64'},

            {'raw_name': 'ERA-', 'column_name': 'earned_run_average_minus', 'type': 'int64'},
            {'raw_name': 'FIP-', 'column_name': 'fielding_independent_pitching_minus', 'type': 'int64'},
            {'raw_name': 'xFIP-', 'column_name': 'expected_fielding_independent_pitching_minus', 'type': 'int64'},

        ] -%}

    {%- else -%}
        {%- set additional_columns = [] -%}
    {%- endif -%}
    
    {{ return(columns) }}

{%- endmacro -%}