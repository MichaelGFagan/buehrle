with player_awards as (

    select * from {{ source('lahman', 'awards_players') }}

),

manager_awards as (

    select * from {{ source('lahman', 'awards_managers') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

unioned as (

    select *, 'player' as type from player_awards
    union all
    select *, 'manager' as type from manager_awards

),

transform as (

    select
        cast(unioned.yearid as int64) as year_id
      , unioned.lgid as league_id
      , unioned.awardid as name
      , lookup.person_id
      , unioned.type
      , case
            when unioned.tie = 'Y' then true
            when unioned.tie = 'N' then false
        end as is_tie
      , unioned.notes
      , row_number() over (partition by year_id, league_id, name order by lahman_id) as unique_id

    from unioned
    left join lookup
        on unioned.playerid = lookup.lahman_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'league_id',
            'name',
            'unique_id']) }} as award_tie_id
      , {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'league_id',
            'name']) }} as award_id
      , * exclude (unique_id)

    from transform

)

select * from final