with source as (

    select * from {{ source('lahman', 'college_playing') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

transform as (

    select
        lookup.person_id
      , source.yearid as year_id
      , source.schoolid as school_id

    from source
    left join lookup
        on source.playerid = lookup.lahman_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'person_id',
            'year_id',
            'school_id']) }} as player_college_season_id
      , *

    from transform

)

select * from final