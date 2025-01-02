with source as (

    select * from {{ source('lahman', 'home_games') }}

),

renamed as (

    select
        yearkey::int as year_id
      , leaguekey as league_id
      , teamkey as team_id
      , parkkey as park_id
      , spanfirst::date as first_game_date
      , spanlast::date as last_game_date
      , games::int games
      , openings::int as dates_played
      , attendance::int as attendance

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'team_id',
            'park_id']) }} as team_home_games_id
      , *

    from renamed

)

select * from final