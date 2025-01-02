with pitching as (

    select
        * exclude(stint)
      , stint
      , false::boolean as is_postseason
      
    from {{ source('lahman', 'pitching') }}

),

pitching_post as (

    select
        * exclude(round)
      , round as stint
      , true::boolean as is_postseason
      
    from {{ source('lahman', 'pitching_post') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

unioned as (

    select * from pitching
    union all
    select * from pitching_post

),

renamed as (

    select
        unioned.yearid::int as year_id
      , unioned.lgid as league_id
      , unioned.teamid as team_id
      , unioned.stint
      , unioned.is_postseason
      , unioned.playerid as person_id
      , unioned.w::int as wins
      , unioned.l::int as losses
      , unioned.g::int as games
      , unioned.gs::int as games_started
      , unioned.gf::int as games_finished
      , unioned.cg::int as complete_games
      , unioned.sho::int as shutouts
      , unioned.sv::int as saves
      , unioned.ipouts::int as outs_pitched
      , unioned.h::int as hits
      , unioned.r::int as runs
      , unioned.er::int as earned_runs
      , unioned.hr::int as home_runs
      , unioned.bb::int as walks
      , unioned.so::int as strikeouts
      , unioned.ibb::int as intentional_walks
      , unioned.wp::int as wild_pitches
      , unioned.hbp::int as hit_by_pitches
      , unioned.bk::int as balks
      , unioned.bfp::int as batters_faced
      , unioned.sh::int as sacrifice_hits
      , unioned.sf::int as sacrifice_flies
      , unioned.gidp::int as ground_into_double_plays

    from unioned
    left join lookup
        on unioned.playerid = lookup.lahman_id

),

transform as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'team_id',
            'stint',
            'is_postseason',
            'person_id']) }} as year_team_stint_season_player_id
      , {{ dbt_utils.generate_surrogate_key([
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
      , year_id
      , league_id
      , team_id
      , stint
      , is_postseason
      , person_id
      , wins
      , losses
      , games as games_pitched
      , games_started
      , games_finished
      , complete_games
      , shutouts
      , saves
      , batters_faced
      , outs_pitched
      , {{ convert_outs_to_innings('outs_pitched') }} as innings_pitched
      , hits as hits_allowed
      , runs as runs_allowed
      , earned_runs as earned_runs_allowed
      , home_runs as home_runs_allowed
      , walks as walks_allowed
      , strikeouts as strikeouts_thrown
      , intentional_walks as intentional_walks_allowed
      , hit_by_pitches as hit_by_pitches_allowed
      , wild_pitches as wild_pitches_allowed
      , balks
      , sacrifice_hits as sacrifice_hits_allowed
      , sacrifice_flies as sacrifice_flies_allowed
      , ground_into_double_plays as ground_into_double_plays_induced
      , {{ safe_divide('hits', 'batters_faced - walks - hit_by_pitches - sacrifice_hits - sacrifice_flies', 3) }} as batting_average_against
      , {{ safe_divide('hits - home_runs', 'batters_faced - walks - hit_by_pitches - sacrifice_hits - strikeouts - home_runs', 3) }} as batting_average_on_balls_in_play
      , {{ safe_divide('runs * 27', 'outs_pitched', 2) }} as runs_allowed_average
      , {{ safe_divide('earned_runs * 27', 'outs_pitched', 2) }} as earned_runs_allowed_average
        -- FIP: (13 * home_runs + 3 * (walks + hit_by_pitches) - 2 * strikeouts) / batters_faced * 27 + constant as fielding_independent_pitching,
      , {{ safe_divide('walks + hits * 3', 'outs_pitched', 3) }} as whip
      , {{ safe_divide('hits * 27', 'outs_pitched', 1) }} as hits_allowed_per_nine
      , {{ safe_divide('home_runs * 27', 'outs_pitched', 1) }} as home_runs_allowed_per_nine
      , {{ safe_divide('walks * 27', 'outs_pitched', 1) }} as walks_allowed_per_nine
      , {{ safe_divide('walks', 'strikeouts', 2) }} as walks_allowed_per_strikeout_thrown
      , {{ safe_divide('strikeouts', 'batters_faced', 3) }} as strikeouts_thrown_rate
      , {{ safe_divide('walks', 'batters_faced', 3) }} as walks_allowed_rate
      , {{ safe_divide('home_runs', 'batters_faced', 3) }} as home_runs_allowed_rate

    from renamed

)

select * from transform