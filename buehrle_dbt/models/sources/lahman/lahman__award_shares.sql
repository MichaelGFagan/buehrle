with players as (

    select * from {{ source('lahman', 'award_shares_players') }}

),

managers as (

    select * from {{ source('lahman', 'award_shares_managers') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

unioned as (

    select * from players
    union all
    select * from managers

),

transform as (

    select
        cast(unioned.yearid as int64) as year_id
      , unioned.lgid as league_id
      , unioned.awardid as name
      , lookup.person_id
      , cast(unioned.pointswon as int64) as points
      , cast(unioned.votesfirst as int64) as first_place_votes
      , cast(unioned.pointsmax as int64) as max_points

    from unioned
    left join lookup
        on unioned.playerID = lookup.lahman_id

)

, final as (


    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'league_id',
            'name',
            'person_id']) }} as award_share_id
      , {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'league_id',
            'name']) }} as award_id
      , transform.*

    from transform

)

select * from final