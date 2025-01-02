with source as (

    select * from {{ source('lahman', 'appearances') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

renamed as (

    select
        lookup.person_id
      , source.lgid as league_id
      , source.teamid as team_id
      , source.yearid::int as year_id
      , false::boolean as is_postseason
      , source.g_all as games
      , source.gs as games_started
      , source.g_batting as games_at_batter
      , source.g_defense as games_at_fielder
      , source.g_p as games_at_pitcher
      , source.g_c as games_at_catcher
      , source.g_1b as games_at_first_base
      , source.g_2b as games_at_second_base
      , source.g_3b as games_at_third_base
      , source.g_ss as games_at_shortstop
      , source.g_lf as games_at_left_field
      , source.g_cf as games_at_center_field
      , source.g_rf as games_at_right_field
      , source.g_of as games_at_outfielder
      , source.g_dh as games_at_designated_hitter
      , source.g_ph as games_at_pinch_hitter
      , source.g_pr as games_at_pinch_runner

    from source
    left join lookup
        on source.playerid = lookup.lahman_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'team_id',
            'is_postseason',
            'person_id']) }} as year_team_season_player_id
      , {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'is_postseason',
            'person_id']) }} as year_season_player_id
      , {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'team_id',
            'person_id']) }} as year_team_player_id
      , {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'person_id']) }} as year_player_id
      , *

    from renamed

)

select * from final