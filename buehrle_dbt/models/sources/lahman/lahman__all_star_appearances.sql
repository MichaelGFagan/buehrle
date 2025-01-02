with source as (

    select * from {{ source('lahman', 'all_star_full') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

transform as (

    select
        cast(source.yearid as int64) as year_id
      , source.lgid as league_id
      , source.teamid as team_id
      , source.gameid as game_id
      , cast(source.gamenum as int64) as game_number
      , source.playerid as lahman_id
      , source.gp as games_played
      , cast(source.startingpos as int64) as starting_position

    from source
    left join lookup
        on source.playerid = lookup.lahman_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'league_id',
            'team_id',
            'game_id',
            'lahman_id']) }} as all_star_appearance_id
      , {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'league_id',
            'team_id',
            'game_id']) }} as all_star_game_id
      , *

    from transform

)

select * from final