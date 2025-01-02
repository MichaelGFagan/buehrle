with fielding as (

    select * from {{ source('lahman', 'fielding_of') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

transform as (

    select
        fielding.yearID::int as year_id
      , fielding.stint::int as stint
      , lookup.person_id
      , glf::int as games_at_left_field
      , gcf::int as games_at_center_field
      , grf::int as games_at_right_field

    from fielding
    left join lookup
        on fielding.playerID = lookup.lahman_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'stint',
            'person_id']) }} as year_stint_player_id
      , {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'person_id']) }} as year_player_id
      , *

    from transform

)

select * from final