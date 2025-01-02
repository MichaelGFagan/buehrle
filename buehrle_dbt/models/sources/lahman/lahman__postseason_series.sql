with source as (

    select * from {{ source('lahman', 'series_post') }}

),

transform as (

    select
        yearid as year_id
      , round
      , teamidwinner as winner_team_id
      , lgidwinner as winner_league_id
      , teamidloser as loser_team_id
      , lgidloser as loser_league_id
      , wins::int as wins
      , losses::int as losses
      , ties::int as ties

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'round',
            'winner_team_id',
            'loser_team_id']) }} as postseason_series_id
      , *

    from transform

)

select * from final