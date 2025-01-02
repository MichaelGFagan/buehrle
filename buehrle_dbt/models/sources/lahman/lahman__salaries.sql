with source as (

    select * from {{ source('lahman', 'salaries') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

transform as (

    select
        source.yearid::int as year_id
      , source.lgid as league_id
      , source.teamid as team_id
      , lookup.person_id
      , source.salary::int as salary

    from source
    left join lookup
        on source.playerid = lookup.lahman_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'team_id',
            'person_id']) }} as year_team_player_id
      , *

    from transform

)

select * from final