with fielding as (

    select
        * exclude (stint, wp)
      , wp
      , stint
      , null as tp
      , false::boolean as is_postseason

    from {{ source('lahman', 'fielding') }}

    where 1 = 1
        and not (yearid::int >= 1954 and pos = 'OF')

),

fielding_outfield_split as (

    select
        * exclude (stint, wp)
      , wp
      , stint
      , null as tp
      , false::boolean as is_postseason

    from {{ source('lahman', 'fielding_of_split') }}

    

),

fielding_postseason as (

    select
        * exclude (round, tp)
      , null as zr
      , null as wp
      , round as stint
      , tp
      , true::boolean as is_postseason
    
    from {{ source('lahman', 'fielding_post') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

unioned as (

    select * from fielding
    union all
    select * from fielding_outfield_split
    union all
    select * from fielding_postseason

),

transform as (

    select
        unioned.yearid::int as year_id
      , unioned.lgid as league_id
      , unioned.teamid as team_id
      , unioned.stint
      , unioned.is_postseason
      , lookup.person_id
      , unioned.pos as position
      , case unioned.pos
            when 'P' then 1
            when 'C' then 2
            when '1B' then 3
            when '2B' then 4
            when '3B' then 5
            when 'SS' then 6
            when 'LF' then 7
            when 'CF' then 8
            when 'RF' then 9
        end as position_number
      , unioned.g::int as games
      , unioned.gs::int as games_started
      , unioned.innouts::int as outs_at_position
      , {{ convert_outs_to_innings('outs_at_position') }} as innings
      , unioned.po::int as putouts
      , unioned.a::int as assists
      , unioned.e::int as errors
      , unioned.dp::int as double_plays
      , unioned.tp::int as triple_plays
      , unioned.pb::int as passed_balls
      , unioned.wp::int as wild_pitches
      , unioned.sb::int as stolen_bases_allowed
      , unioned.cs::int as caught_stealing
      , unioned.zr::int as zone_rating

    from unioned
    left join lookup
        on unioned.playerid = lookup.lahman_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'team_id',
            'stint',
            'person_id',
            'position']) }} as year_team_stint_player_position_id 
      , {{ dbt_utils.generate_surrogate_key([
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
      , is_postseason
      , person_id
      , position
      , position_number
      , games
      , games_started
      , outs_at_position
      , putouts
      , assists
      , errors
      , double_plays
      , triple_plays
      , passed_balls
      , wild_pitches
      , stolen_bases_allowed
      , caught_stealing
      , zone_rating

    from transform

)

select * from final

