with batting as (

    select * from {{ source('baseball_reference', 'batting_war') }}

),

chadwick as (

    select * from {{ ref('chadwick__register') }}

),

transformed as (
    
    select
        {{ dbt_utils.generate_surrogate_key([
            'chadwick.person_id', 
            'batting.stint_id', 
            'batting.team_id', 
            'batting.year_id', 
            'FALSE']) }} as player_stint_year_id
        , {{ dbt_utils.generate_surrogate_key([
            'chadwick.person_id', 
            'batting.team_id', 
            'batting.year_id']) }} as player_year_team_id
        , {{ dbt_utils.generate_surrogate_key([
            'chadwick.person_id', 
            'batting.year_id']) }} as player_year_id
        , chadwick.person_id
        , batting.mlb_id as mlbam_id
        , batting.player_id as baseball_reference_id
        , batting.stint_id as stint
        , batting.team_id
        , batting.lg_id as league_id
        , batting.year_id
        , false as is_postseason
        , cast(
            case
                when batting.pitcher = 'Y' then true
                when batting.pitcher = 'N' then false
                else null
            end
          as boolean) as is_pitcher
        , batting.age::bigint as age
        , batting.pa::bigint as plate_appearances
        , batting.g::bigint as games_played
        , batting.inn::decimal(6,1) as innings
        , batting.runs_bat::decimal(6,2) as runs_batting
        , batting.runs_br::decimal(6,2) as runs_baserunning
        , batting.runs_dp::decimal(6,2) as runs_ground_into_double_play
        , batting.runs_field::decimal(6,2) as runs_fielding
        , batting.runs_infield::decimal(6,2) as runs_infield
        , batting.runs_outfield::decimal(6,2) as runs_outfield
        , batting.runs_catcher::decimal(6,2) as runs_catcher
        , batting.runs_good_plays::decimal(6,2) as runs_good_plays
        , batting.runs_defense::decimal(6,2) as runs_defense
        , batting.runs_position::decimal(6,2) as runs_position
        , batting.runs_position_p::decimal(6,2) as runs_position_pitcher_adjustment
        , batting.runs_replacement::decimal(6,2) as runs_replacement
        , batting.runs_above_rep::decimal(6,2) as runs_above_replacement
        , batting.runs_above_avg::decimal(6,2) as runs_above_average
        , batting.runs_above_avg_off::decimal(6,2) as runs_above_average_offense
        , batting.runs_above_avg_def::decimal(6,2) as runs_above_average_defense
        , batting.waa::decimal(6,2) as wins_above_average
        , batting.waa_off::decimal(6,2) as wins_above_average_offense
        , batting.waa_def::decimal(6,2) as wins_above_average_defense
        , batting.war::decimal(6,2) as wins_above_replacement
        , batting.war_off::decimal(6,2) as wins_above_replacement_offense
        , batting.war_def::decimal(6,2) as wins_above_replacement_defense
        , batting.war_rep::decimal(6,2) as replacement_player_wins_above_replacement
        , batting.salary::bigint as salary
        , batting.teamrpg::decimal(10,5) as average_team_runs_scored_per_game
        , batting.opprpg::decimal(10,5) as average_team_runs_allowed_per_game
        , batting.opprppa_rep::decimal(10,5) as average_team_runs_allowed_per_plate_appearance_against_replacement  -- noqa: L016
        , batting.opprpg_rep::decimal(10,5) as average_team_runs_allowed_per_game_against_replacement  -- noqa: L016
        , batting.pyth_exponent::decimal(7,3) as pythagenpat_exponent
        , batting.pyth_exponent_rep::decimal(7,3) as pythagenpat_exponent_replacement
        , batting.waa_win_perc::decimal(8,4) as wins_above_average_win_percentage_with_average_team  -- noqa: L016
        , batting.waa_win_perc_off::decimal(8,4) as wins_above_average_win_percentage_with_average_team_offense  -- noqa: L016
        , batting.waa_win_perc_def::decimal(8,4) as wins_above_average_win_percentage_with_average_team_defense  -- noqa: L016
        , batting.waa_win_perc_rep::decimal(8,4) as wins_above_average_win_percentage_with_average_team_replacement  -- noqa: L016
        , batting.ops_plus::decimal(14,10) as on_base_plus_slugging_plus
        , batting.tob_lg::decimal(7,3) as leage_average_player_times_on_base
        , batting.tb_lg::decimal(7,3) as league_average_player_total_bases
    
    from batting
    left join chadwick
        on batting.player_id = chadwick.baseball_reference_id

)

select * from transformed