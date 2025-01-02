with source as (

    select * from {{ source('lahman', 'managers_half') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

transform as (

    select
        source.yearid::int as year_id
      , source.lgid as league_id
      , source.teamid as team_id
      , source.inseason as order_in_season
      , source.half as season_half
      , lookup.person_id
      , source.g::int as games
      , source.w::int as wins
      , source.l::int as losses
      , source.rank::int as position_in_standings

    from source
    left join lookup
        on source.playerid = lookup.lahman_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'team_id',
            'order_in_season',
            'season_half']) }} as year_team_order_split_id
      , {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'team_id',
            'person_id',
            'season_half']) }} as year_team_manager_split_id
      , *

    from transform

)

select * from final