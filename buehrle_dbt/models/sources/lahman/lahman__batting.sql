with batting as (

    select
        * exclude(playerid, yearid, stint, g_batting, g_old)
      , yearid
      , playerid
      , stint
      , g_batting
      , false::boolean as is_postseason
      
    from {{ source('lahman', 'batting') }}

),

batting_post as (

    select
        * exclude(playerid, yearid, round)
      , yearid
      , playerid
      , round as stint
      , null as g_batting
      , true::boolean as is_postseason
      
    from {{ source('lahman', 'batting_post') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

unioned as (

    select * from batting
    union all
    select * from batting_post

),

renamed as (

    select
        unioned.yearid::int as year_id
      , unioned.lgid as league_id
      , unioned.teamid as team_id
      , unioned.stint as stint
      , unioned.is_postseason
      , lookup.person_id
      , ifnull(unioned.g::int, 0) as games
      , ifnull(unioned.g_batting::int, 0) as games_batting
      , ifnull(unioned.ab::int, 0) as at_bats
      , ifnull(unioned.r::int, 0) as runs
      , ifnull(unioned.h::int, 0) as hits
      , ifnull(unioned._2b::int, 0) as doubles
      , ifnull(unioned._3b::int, 0) as triples
      , ifnull(unioned.hr::int, 0) as home_runs
      , ifnull(unioned.rbi::int, 0) as runs_batted_in
      , ifnull(unioned.sb::int, 0) as stolen_bases
      , ifnull(unioned.cs::int, 0) as caught_stealing
      , ifnull(unioned.bb::int, 0) as walks
      , ifnull(unioned.so::int, 0) as strikeouts
      , ifnull(unioned.ibb::int, 0) as intentional_walks
      , ifnull(unioned.hbp::int, 0) as hit_by_pitches
      , ifnull(unioned.sh::int, 0) as sacrifice_hits
      , ifnull(unioned.sf::int, 0) as sacrifice_flies
      , ifnull(unioned.gidp::int, 0) as ground_into_double_plays

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
            'person_id']) }} as year_team_stint_player_id
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
      , person_id 
      , is_postseason
      , games
      , games_batting
      , at_bats + walks + hit_by_pitches + sacrifice_hits + sacrifice_flies as plate_appearances
      , at_bats
      , runs
      , hits
      , doubles
      , triples
      , home_runs
      , hits + walks + hit_by_pitches as times_on_base
      , {{ safe_divide('times_on_base', 'at_bats + walks + hit_by_pitches + sacrifice_flies', 3) }} as on_base_percentage
      , (at_bats - hits) + sacrifice_hits + sacrifice_flies + ground_into_double_plays + caught_stealing as outs_made
      , doubles + triples + home_runs as extra_base_hits
      , hits + doubles + (2 * triples) + (3 * home_runs) as total_bases
      , runs_batted_in
      , stolen_bases
      , caught_stealing
      , walks
      , strikeouts
      , intentional_walks
      , hit_by_pitches
      , sacrifice_hits
      , sacrifice_flies
      , ground_into_double_plays

    from renamed

)

select * from transform