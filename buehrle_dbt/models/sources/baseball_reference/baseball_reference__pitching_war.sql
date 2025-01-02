with pitching as (

    select * from {{ source('baseball_reference', 'pitching_war') }}

),

chadwick as (

    select * from {{ ref('chadwick__register') }}

),

transformed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'chadwick.person_id',
            'pitching.stint_id',
            'pitching.team_id',
            'pitching.year_id',
            'FALSE']) }} as player_stint_year_id
      , {{ dbt_utils.generate_surrogate_key([
            'chadwick.person_id',
            'pitching.team_id',
            'pitching.year_id']) }} as player_year_team_id
      , {{ dbt_utils.generate_surrogate_key([
            'chadwick.person_id',
            'pitching.year_id',]) }} as player_year_id
      , chadwick.person_id
      , pitching.mlb_id as mlbam_id
      , pitching.player_id as baseball_reference_id
      , pitching.stint_id as stint
      , pitching.team_id
      , pitching.lg_id as league_id
      , pitching.year_id
      , FALSE as is_postseason
      , pitching.age::bigint as age
      , pitching.g::bigint as games
      , pitching.gs::bigint as games_started
      , pitching.ipouts::bigint as outs_pitched
      , pitching.ipouts_start::bigint as outs_pitched_as_starter
      , pitching.ipouts_relief::bigint as outs_pitched_as_reliever
      , pitching.ra::bigint as runs_allowed
      , pitching.xra::decimal(7,3) as expected_runs_allowed
      , pitching.xra_sprp_adj::decimal(7,3) as expected_runs_allowed_role_adjustment
      , pitching.xra_extras_adj::decimal(7,3) as expected_runs_allowed_extra_adjustment
      , pitching.xra_def_pitcher::decimal(7,3) as expected_runs_allowed_team_defense_adjustment
      , pitching.ppf::bigint as home_park_factor
      , pitching.ppf_custom::bigint as pitched_park_factor
      , pitching.xra_final::decimal(7,3) as expected_runs_allowed_final
      , pitching.bip::bigint as balls_in_play
      , pitching.bip_perc::decimal(7,3) as percent_of_teams_balls_in_play
      , pitching.rs_def_total::decimal(7,1) as team_defensive_runs_saved
      , pitching.runs_above_avg::decimal(7,3) as runs_above_average
      , pitching.runs_above_avg_adj::decimal(7,3) as runs_above_average_adjusted
      , pitching.runs_above_rep::decimal(7,3) as runs_above_average_multiplied_by_replacement_level_factor
      , pitching.rpo_replacement::decimal(5,3) as runs_per_out_replacement_level
      , pitching.gr_leverage_index_avg::decimal(5,2) as leverage_in_relief_appearances
      , pitching.war::decimal(5,2) as wins_above_replacement
      , pitching.war_rep::decimal(6,4) as replacement_pitcher_wins_above_replacement
      , pitching.salary::bigint as salary
      , pitching.teamrpg::decimal(10,5) as average_team_runs_per_game_with_pitcher
      , pitching.opprpg::decimal(10,5) as average_opponent_runs_per_game
      , pitching.opprpg_rep::decimal(10,5) as average_opponent_runs_allowed_per_game_against_replacement
      , pitching.pyth_exponent::decimal(7,3) as pythagenpat_exponent
      , pitching.waa::decimal(7,4) as wins_above_average
      , pitching.waa_adj::decimal(7,4) as wins_above_average_adjusted
      , pitching.waa_win_perc::decimal(7,4) as wins_above_average_win_percentage_with_average_team
      , pitching.era_plus::decimal(6,1) as era_plus
      , pitching.er_lg::decimal(7,3) as league_average_pitcher_earned_runs

    from pitching
    left join chadwick
        on pitching.player_id = chadwick.baseball_reference_id

)

select * from transformed
