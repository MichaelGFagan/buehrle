{{
    config(
        enabled=false
    )
}}

with lahman as (

    select * from {{ ref('lahman__teams') }}
)

, fangraphs as (

    select * from {{ source('utils', 'team_ids') }}

)

, transform as (

    select
        yearID as year_id
      , lgID as league_id
      , teamID as team_id
      , franchID as franchise_id
      , teamIDfg as fangraphs_team_id
      , teamIDBR as baseball_reference_team_id
      , teamIDretro as retrosheet_team_id

    from teams
    left join fangraphs

)

, final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'team_id']) }} as year_team_id
      , *

    from transform

)

select * from final