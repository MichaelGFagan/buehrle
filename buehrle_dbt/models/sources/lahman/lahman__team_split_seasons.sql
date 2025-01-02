with source as (

    select * from {{ source('lahman', 'teams_half') }}

),

transform as (

    select
        yearid::int as year_id
      , lgid as league_id
      , divid as division_id
      , half as season_half
      , teamid as team_id
      , rank as position_in_standings
      , g::int as games
      , w::int as wins
      , l::int as losses
      , case
            when divwin = 'Y' then true::boolean
            when divwin = 'N' then false::boolean
        end as is_division_winner

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'league_id',
            'division_id',
            'season_half',
            'team_id']) }} as team_split_season_id
      , *

    from transform

)

select * from final